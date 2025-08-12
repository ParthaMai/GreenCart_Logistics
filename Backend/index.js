const express = require('express');
const fs = require('fs');
const path = require('path');
const main = require('./server');

const app = express();
const PORT = process.env.PORT || 3000;
const OUTPUT_CSV = path.join(__dirname, 'assignments.csv');

app.get('/', async (req, res) => {
  try {
    await main();
    res.send(`
      <h2>CSV assignment completed successfully!</h2>
      <p><a href="/download">Download assignments.csv</a></p>
    `);
  } catch (err) {
    res.status(500).send('Error: ' + err.message);
  }
});

app.get('/download', (req, res) => {
  if (fs.existsSync(OUTPUT_CSV)) {
    res.download(OUTPUT_CSV);
  } else {
    res.status(404).send('File not found.');
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
