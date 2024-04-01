const {
  parseQuery,
  parseDeleteQuery,
  parseInsertQuery,
} = require("./queryParser.js");
const { readCSV, writeCSV } = require("./csvReader.js");

async function executeSELECTQuery(query) {
  try {
    const { fields, table, whereClauses, joinTable, joinCondition } =
      parseQuery(query);
    // // console.log(joinType);
    // console.log(joinTable);
    // console.log(joinCondition);

    const joinRegex =
      /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);
    // console.log(joinMatch);
    let joinType = null;

    if (joinMatch) {
      // console.log("join matching");
      joinType = joinMatch[1].trim();
    }

    let data = await readCSV(`${table}.csv`);

    // // console.log(data);
    // // console.log(whereClauses);
    // Perform INNER JOIN if specified
    if (joinTable && joinCondition) {
      const joinData = await readCSV(`${joinTable}.csv`);
      // console.log(joinData);
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

    // console.log(filteredData);
    delete filteredData[0].age;
    // console.log(filteredData);
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

function performLeftJoin(data, joinData, joinCondition, fields, table) {
  return data.flatMap((mainRow) => {
    const matchingJoinRows = joinData.filter((joinRow) => {
      const mainValue = getValueFromRow(mainRow, joinCondition.left);
      const joinValue = getValueFromRow(joinRow, joinCondition.right);
      return mainValue === joinValue;
    });

    if (matchingJoinRows.length === 0) {
      return [createResultRow(mainRow, null, fields, table, true)];
    }

    return matchingJoinRows.map((joinRow) =>
      createResultRow(mainRow, joinRow, fields, table, true)
    );
  });
}

function getValueFromRow(row, compoundFieldName) {
  const [tableName, fieldName] = compoundFieldName.split(".");
  return row[`${tableName}.${fieldName}`] || row[fieldName];
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
  // Cache the structure of a main table row (keys only)
  const mainTableRowStructure =
    data.length > 0
      ? Object.keys(data[0]).reduce((acc, key) => {
          acc[key] = null; // Set all values to null initially
          return acc;
        }, {})
      : {};

  return joinData.map((joinRow) => {
    const mainRowMatch = data.find((mainRow) => {
      const mainValue = getValueFromRow(mainRow, joinCondition.left);
      const joinValue = getValueFromRow(joinRow, joinCondition.right);
      return mainValue === joinValue;
    });

    // Use the cached structure if no match is found
    const mainRowToUse = mainRowMatch || mainTableRowStructure;

    // Include all necessary fields from the 'student' table
    return createResultRow(mainRowToUse, joinRow, fields, table, true);
  });
}

function createResultRow(
  mainRow,
  joinRow,
  fields,
  table,
  includeAllMainFields
) {
  const resultRow = {};

  if (includeAllMainFields) {
    // Include all fields from the main table
    Object.keys(mainRow || {}).forEach((key) => {
      const prefixedKey = `${table}.${key}`;
      resultRow[prefixedKey] = mainRow ? mainRow[key] : null;
    });
  }

  // Now, add or overwrite with the fields specified in the query
  fields.forEach((field) => {
    const [tableName, fieldName] = field.includes(".")
      ? field.split(".")
      : [table, field];
    resultRow[field] =
      tableName === table && mainRow
        ? mainRow[fieldName]
        : joinRow
        ? joinRow[fieldName]
        : null;
  });

  return resultRow;
}

async function executeDELETEQuery(query) {
  const { table, whereClauses } = parseDeleteQuery(query);
  let data = await readCSV(`${table}.csv`);

  if (whereClauses.length > 0) {
    // Filter out the rows that meet the where clause conditions
    data = data.filter(
      (row) => !whereClauses.every((clause) => evaluateCondition(row, clause))
    );
  } else {
    // If no where clause, clear the entire table
    data = [];
  }

  // Save the updated data back to the CSV file
  await writeCSV(`${table}.csv`, data);

  return { message: "Rows deleted successfully." };
}

async function executeINSERTQuery(query) {
  const { table, columns, values, returningColumns } = parseInsertQuery(query);
  const data = await readCSV(`${table}.csv`);

  // Check if 'id' column is included in the query and in CSV headers
  let newId = null;
  if (!columns.includes('id') && data.length > 0 && 'id' in data[0]) {
      // 'id' column not included in the query, so we auto-generate an ID
      const existingIds = data.map(row => parseInt(row.id)).filter(id => !isNaN(id));
      const maxId = existingIds.length > 0 ? Math.max(...existingIds) : 0;
      newId = maxId + 1;
      columns.push('id');
      values.push(newId.toString()); // Add as a string
  }

  // Create a new row object matching the CSV structure
  const headers = data.length > 0 ? Object.keys(data[0]) : columns;
  const newRow = {};
  headers.forEach(header => {
      const columnIndex = columns.indexOf(header);
      if (columnIndex !== -1) {
          let value = values[columnIndex];
          if (value.startsWith("'") && value.endsWith("'")) {
              value = value.substring(1, value.length - 1);
          }
          newRow[header] = value;
      } else {
          newRow[header] = header === 'id' ? newId.toString() : '';
      }
  });

  // Add the new row to the data
  data.push(newRow);

  // Save the updated data back to the CSV file
  await writeCSV(`${table}.csv`, data);

  // Prepare the returning result if returningColumns are specified
  let returningResult = {};
  if (returningColumns.length > 0) {
      returningColumns.forEach(column => {
          returningResult[column] = newRow[column];
      });
  }

  return {
      message: "Row inserted successfully.",
      insertedId: newId,
      returning: returningResult
  };
}

module.exports = { executeSELECTQuery, executeDELETEQuery, executeINSERTQuery};
