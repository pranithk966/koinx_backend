const express = require('express')
const cors = require('cors')
const multer = require('multer')
const csv = require('csv-parser')
const mongoose = require('mongoose')
const fs = require('fs')
const moment = require('moment')
const Trade = require('./models/tradeModel')
require('dotenv').config()

const app = express()

app.use(cors())
app.use(express.json())

const upload = multer({ dest: 'uploads/' })

app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).send('No file uploaded')
  }

  const trades = []

  fs.createReadStream(req.file.path)
    .pipe(csv())
    .on('data', (row) => {
      const [base_coin, quote_coin] = row['Market'].split('/')
      trades.push({
        timeStamp: new Date(row['UTC_Time']),
        operation: row['Operation'],
        base_coin: base_coin,
        quote_coin: quote_coin,
        amount: parseFloat(row['Buy/Sell Amount']),
        price: parseFloat(row['Price']),
      })
    })
    .on('end', async () => {
      try {
        await Trade.insertMany(trades)
        res
          .status(200)
          .send('CSV data successfully uploaded and saved to the database')
      } catch (err) {
        res.status(500).send('int')
      }
    })
})

app.post('/balance', async (req, res) => {
  const { timestamp } = req.body

  if (!timestamp) {
    return res.status(400).send('Timestamp is required')
  }

  const endTime = moment(timestamp, 'YYYY-MM-DD HH:mm:ss', true)

  if (!endTime.isValid()) {
    return res
      .status(400)
      .send('Invalid timestamp format. Use YYYY-MM-DD HH:mm:ss')
  }

  try {
    const endDate = endTime.toDate()

    const trades = await Trade.find({ timeStamp: { $lt: endDate } })

    const balances = {}

    trades.forEach((trade) => {
      const { base_coin, operation, amount } = trade

      const op = operation.toUpperCase()

      if (!balances[base_coin]) {
        balances[base_coin] = 0
      }

      if (op === 'BUY') {
        balances[base_coin] += amount
      } else if (op === 'SELL') {
        balances[base_coin] -= amount
      }
    })

    Object.keys(balances).forEach((coin) => {
      if (balances[coin] === 0) {
        delete balances[coin]
      }
    })

    res.status(200).json(balances)
  } catch (err) {
    console.error(err)
    res.status(500).send('Error fetching balances')
  }
})

app.listen(process.env.PORT || 8080, () => {
  console.log('listening on the port', 8080)
})

mongoose
  .connect(process.env.MONGO_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
    connectTimeoutMS: 30000,
    bufferCommands: false,
  })

  .then(() => {
    console.log('database connected')
  })
  .catch((err) => console.log(err))
