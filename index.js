const moment = require("moment");
const axios = require("axios");
const fs = require("fs");
const readline = require("readline");

const cdcApiEndPoint = "https://data.cdc.gov/resource/qjju-smys.json";
const countyFipsFilepath = "data/county_fips.csv";
const stateFipsFilepath = "data/state_fips.csv";
const NUM_OF_COUNTIES = 3109;
const NUM_OF_DAYS = 7;

let responseCounter = 0;
let allFipsPM25 = {};


displayInputPrompt();


function displayInputPrompt() {
  console.log("Get county with best air quality for a given date");

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "\nEnter date (MM-DD-YYYY) or Q to quit: "
  });

  rl.prompt();

  rl.on("line", (line) => {
    const input = line.trim().toUpperCase();
    if (input === "Q") {
      console.log("\n");
      rl.close();
      process.exit(0);
    }

    if (moment(input, "MM-DD-YYYY").isValid()) {
      resetGlobalVars();
      run(input, rl);
    } else {
      console.log("\nInvalid date! Try again\n");
      rl.prompt();
    }
  });
}

function resetGlobalVars() {
  responseCounter = 0;
  allFipsPM25 = {};
  // console.log(responseCounter, allFipsPM25);
}

function run(inputDateStr, rl) {
  const inputDate = moment(inputDateStr, "MM-DD-YYYY");

  getAirQualityData(inputDate, rl);
  getAirQualityData(inputDate.add(1, "d"), rl);
  getAirQualityData(inputDate.add(1, "d"), rl);
  getAirQualityData(inputDate.add(1, "d"), rl);
  getAirQualityData(inputDate.add(1, "d"), rl);
  getAirQualityData(inputDate.add(1, "d"), rl);
  getAirQualityData(inputDate.add(1, "d"), rl);
}

async function getAirQualityData(qryDate, rl) {
  try {
    const filterDate = formatFilterDate(qryDate);
    const response = await axios.get(`${cdcApiEndPoint}?$limit=${NUM_OF_COUNTIES}&date=${filterDate}`);
    // console.log(`${filterDate}: ${response.data.length}`);
    importFipsPM25Data(response.data);
    responseCounter++;

    if (responseCounter === NUM_OF_DAYS) {
      // Responses for all days have been received from the API
      displayCountyWithBestPM25(rl);
    }
  } catch(err) {
    console.log(err);
  }
}

function formatFilterDate(filterDate) {
  return filterDate.format("DDMMMYYYY").toUpperCase();
}

function importFipsPM25Data(cdcData) {
  for (let item of cdcData) {
    const currentFipsCode = formatFipsCode(item.countyfips);
    const newVal = parseFloat(item.pm_mean_pred);

    if (!allFipsPM25.hasOwnProperty(currentFipsCode)) {
      allFipsPM25[currentFipsCode] = { total: newVal, count: 1};
    } else {
      const currentTotal = allFipsPM25[currentFipsCode].total;
      const currentCount = allFipsPM25[currentFipsCode].count;
      const newTotal = currentTotal + newVal;
      const newCount = currentCount + 1;
      allFipsPM25[currentFipsCode].total = newTotal;
      allFipsPM25[currentFipsCode].count = newCount;
    }
  }
}

function formatFipsCode(fipsCode) {
  if (fipsCode.length < 5) {
    const prefixZeroCount = 5 - fipsCode.length;
    return "0".repeat(prefixZeroCount) + fipsCode;
  }

  return fipsCode;
}

async function displayCountyWithBestPM25(rl) {
  const { minPM25, fipsWithMinPM25 } = getMinPM25WithFips();

  try {
    const fipsCountyDataRaw = await readFileAsArray(countyFipsFilepath);
    const fipsStateDataRaw = await readFileAsArray(stateFipsFilepath);
    const fipsCountyTable = importFipsCountyData(fipsCountyDataRaw);
    const fipsStateTable = importFipsStateData(fipsStateDataRaw);
    const fipsStateCode = fipsWithMinPM25.slice(0,2);

    if (fipsCountyTable.hasOwnProperty(fipsWithMinPM25) && fipsStateTable.hasOwnProperty(fipsStateCode)) {
      console.log(`Output: ${fipsCountyTable[fipsWithMinPM25]}, ${fipsStateTable[fipsStateCode]}, PM2.5: ${minPM25}`);
    } else if (fipsStateTable.hasOwnProperty(fipsStateCode)) {
      console.log(`Output: ${fipsWithMinPM25}, ${fipsStateTable[fipsStateCode]}, PM2.5: ${minPM25}`);
    } else {
      console.log(`Output: ${fipsWithMinPM25}, ${fipsStateCode}, PM2.5: ${minPM25}`);
    }

    rl.prompt();
  } catch (err) {
    console.log(err);
  }
}

function getMinPM25WithFips() {
  let minPM25 = Infinity;
  let fipsWithMinPM25 = null;

  for (let fipsCode of Object.keys(allFipsPM25)) {
    const avgPM25Val = allFipsPM25[fipsCode].total / allFipsPM25[fipsCode].count;
    if (avgPM25Val < minPM25) {
      minPM25 = avgPM25Val;
      fipsWithMinPM25 = fipsCode;
    }
  }

  return { minPM25, fipsWithMinPM25 };
}

function readFileAsArray(filepath, removeHeaderRow = true) {
  return new Promise((resolve, reject) => {
    fs.readFile(filepath, "utf8", function(err, data) {
      if (err) {
        return reject(err);
      }

      const rows = data.trim().split("\n");

      if (removeHeaderRow) {
        rows.shift();
      }

      return resolve(rows);
    });
  });
}

function importFipsCountyData(rawData) {
  const fipsCountyTable = {};

  for (let row of rawData) {
    if (row.trim() === "") {
      continue;
    }

    const [fipsCode, countyName] = row.trim().split(",");
    fipsCountyTable[fipsCode.trim()] = countyName.trim();
  }

  return fipsCountyTable;
}

function importFipsStateData(rawData) {
  const fipsStateTable = {};

  for (let row of rawData) {
    if (row.trim() === "") {
      continue;
    }

    const [fipsStateCode, _, stateAbbr] = row.trim().split(",");
    fipsStateTable[fipsStateCode.trim()] = stateAbbr.trim();
  }

  return fipsStateTable;
}
