function parseQuery(query) {
  const whereSplit = query.split(/\sWHERE\s/i);
  const queryWithoutWhere = whereSplit[0]; // Everything before WHERE clause
  const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

  const joinSplit = queryWithoutWhere.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
  const selectPart = joinSplit[0].trim(); // Everything before JOIN clause

  const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
  const selectMatch = selectPart.match(selectRegex);
  if (!selectMatch) {
    throw new Error("Invalid SELECT format");
  }
  let [, fields, table] = selectMatch;

  // Parse WHERE part if it exists
  let whereClauses = [];
  if (whereClause) {
    whereClauses = parseWhereClause(whereClause);
  }

  // Temporarily replace commas within parentheses to avoid incorrect splitting
  const tempPlaceholder = "__TEMP_COMMA__"; // Ensure this placeholder doesn't appear in your actual queries
  fields = fields.replace(/\(([^)]+)\)/g, (match) =>
    match.replace(/,/g, tempPlaceholder)
  );

  const parsedFields = fields
    .split(",")
    .map((field) =>
      field.trim().replace(new RegExp(tempPlaceholder, "g"), ",")
    );

  return {
    fields: parsedFields,
    table: table.trim(),
  };
}

function parseWhereClause(whereString) {
  const conditionRegex = /(.*?)(=|!=|>=|<=|>|<)(.*)/;
  return whereString.split(/ AND | OR /i).map((conditionString) => {
    if (conditionString.includes(" LIKE ")) {
      const [field, pattern] = conditionString.split(/\sLIKE\s/i);
      return {
        field: field.trim(),
        operator: "LIKE",
        value: pattern.trim().replace(/^'(.*)'$/, "$1"),
      };
    } else {
      const match = conditionString.match(conditionRegex);
      if (match) {
        const [, field, operator, value] = match;
        return { field: field.trim(), operator, value: value.trim() };
      }
      throw new Error("Invalid WHERE clause format");
    }
  });
}

module.exports = parseQuery;
