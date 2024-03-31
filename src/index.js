const parseQuery = require("./queryParser.js");
const readCSV = require("./csvReader.js");

async function executeSELECTQuery(query) {
  try {
    const { fields, table, whereClauses, joinTable, joinCondition } =
      parseQuery(query);
    // console.log(joinType);
    console.log(joinTable);
    console.log(joinCondition);

    const joinRegex =
      /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);
    console.log(joinMatch);
    let joinType = null;

    if (joinMatch) {
      console.log("join matching");
      joinType = joinMatch[1].trim();
    }

    let data = await readCSV(`${table}.csv`);

    // console.log(data);
    // console.log(whereClauses);
    // Perform INNER JOIN if specified
    if (joinTable && joinCondition) {
      const joinData = await readCSV(`${joinTable}.csv`);
      console.log(joinData);
      switch (joinType.toUpperCase()) {
        case "INNER":
          data = performInnerJoin(data, joinData, joinCondition, fields, table);
          break;
        case "LEFT":
          data = performLeftJoin(data, joinData, joinCondition, fields, table);
          break;
        case "RIGHT":
          data = performRightJoin(data, joinData, joinCondition, fields, table);
          break;
        default:
          throw new Error(`Unsupported JOIN type: ${joinType}`);
      }
    }
    // Apply WHERE clause filtering after JOIN (or on the original data if no join)
    let filteredData =
      whereClauses.length > 0
        ? data.filter((row) =>
            whereClauses.every((clause) => evaluateCondition(row, clause))
          ) // Filter rows that satisfy all conditions
        : data;

    console.log(filteredData);
    delete filteredData[0].age;
    console.log(filteredData);
    return filteredData;
    
  } catch (error) {
    throw new Error(`Error executing query: ${error.message}`);
  }
}

function performInnerJoin(data, joinData, joinCondition, fields, table) {
  return data.flatMap((mainRow) => {
    return joinData
      .filter((joinRow) => {
        const mainValue = mainRow[joinCondition.left.split(".")[1]];
        const joinValue = joinRow[joinCondition.right.split(".")[1]];
        return mainValue === joinValue;
      })
      .map((joinRow) => {
        return fields.reduce((acc, field) => {
          const [tableName, fieldName] = field.split(".");
          acc[field] =
            tableName === table ? mainRow[fieldName] : joinRow[fieldName];
          return acc;
        }, {});
      });
  });
}

function evaluateCondition(row, clause) {
  let { field, operator, value } = clause;

  // Check if the field exists in the row
  if (row[field] === undefined) {
    throw new Error(`Invalid field: ${field}`);
  }

  // Parse row value and condition value based on their actual types
  const rowValue = parseValue(row[field]);
  let conditionValue = parseValue(value);

  if (operator === "LIKE") {
    // Transform SQL LIKE pattern to JavaScript RegExp pattern
    const regexPattern =
      "^" + value.replace(/%/g, ".*").replace(/_/g, ".") + "$";
    const regex = new RegExp(regexPattern, "i"); // 'i' for case-insensitive matching
    return regex.test(row[field]);
  }

  switch (operator) {
    case "=":
      return rowValue === conditionValue;
    case "!=":
      return rowValue !== conditionValue;
    case ">":
      return rowValue > conditionValue;
    case "<":
      return rowValue < conditionValue;
    case ">=":
      return rowValue >= conditionValue;
    case "<=":
      return rowValue <= conditionValue;
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

function parseValue(value) {
  // Return null or undefined as is
  if (value === null || value === undefined) {
    return value;
  }

  // If the value is a string enclosed in single or double quotes, remove them
  if (
    typeof value === "string" &&
    ((value.startsWith("'") && value.endsWith("'")) ||
      (value.startsWith('"') && value.endsWith('"')))
  ) {
    value = value.substring(1, value.length - 1);
  }

  // Check if value is a number
  if (!isNaN(value) && value.trim() !== "") {
    return Number(value);
  }
  // Assume value is a string if not a number
  return value;
}

module.exports = executeSELECTQuery;
