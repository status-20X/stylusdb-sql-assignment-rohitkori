const parseQuery = require("./queryParser.js");
const readCSV = require("./csvReader.js");

async function executeSELECTQuery(query) {
  const { fields, table, whereClauses } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);
    
  let filteredData;

  if (whereClauses) {
    filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;  
  }
  else {
    filteredData = data;
  }
  
  // Filter the data based on the fields specified in the query
  const result = filteredData.map((row) => {
    const filteredRow = {};
    fields.forEach((field) => {
      if (row.hasOwnProperty(field)) {
        filteredRow[field] = row[field];
      }
    });
    return filteredRow;
  });
  console.log(result);
  return result;
}

module.exports = executeSELECTQuery;
