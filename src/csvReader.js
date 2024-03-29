const fs = require("fs");
const csv = require("csv-parser");
const { parse } = require("json2csv");
const hll = require("hll");

function readCSV(filePath) {
  const results = [];
  var h = hll();

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        resolve(results);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

module.exports = readCSV;