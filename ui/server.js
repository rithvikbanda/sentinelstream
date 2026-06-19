const express = require('express');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));

app.listen(port, () => {
  console.log(`SentinelStream dashboard running at http://localhost:${port}`);
  console.log('Set window.SENTINELSTREAM_API_BASE in the browser console to point at a non-default API address.');
});
