// 🔥 Configure your RapidAPI credentials here
const RAPIDAPI_HOST = 'indian-stock-exchange-api2.p.rapidapi.com';
const RAPIDAPI_KEY = '1c99b13c79msh266bd26283ae7f3p1ded7djsn92d495c38bab';  // ✨ Paste your actual API key here

async function fetchMarketData() {
  try {
    const response = await fetch(`https://${RAPIDAPI_HOST}/gainers`, {
      method: 'GET',
      headers: {
        'X-RapidAPI-Host': RAPIDAPI_HOST,
        'X-RapidAPI-Key': RAPIDAPI_KEY,
      }
    });

    const gainers = await response.json();

    const losersResponse = await fetch(`https://${RAPIDAPI_HOST}/losers`, {
      method: 'GET',
      headers: {
        'X-RapidAPI-Host': RAPIDAPI_HOST,
        'X-RapidAPI-Key': RAPIDAPI_KEY,
      }
    });

    const losers = await losersResponse.json();

    updateTable('gainers-table', gainers);
    updateTable('losers-table', losers);

  } catch (error) {
    console.error("Market Watch Error:", error);
  }
}

function updateTable(tableId, stocks) {
  const tableBody = document.getElementById(tableId);
  tableBody.innerHTML = "";

  if (Array.isArray(stocks)) {
    stocks.slice(0, 10).forEach(stock => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td><b>${stock.symbol || stock.name}</b></td>
        <td>${stock.price || stock.lastPrice}</td>
        <td class="${stock.changePercentage > 0 ? 'text-success' : 'text-danger'}">
          ${stock.changePercentage ? stock.changePercentage.toFixed(2) : stock.pChange}%
        </td>
      `;
      tableBody.appendChild(row);
    });
  } else {
    tableBody.innerHTML = `<tr><td colspan="3" class="text-center text-muted">No data</td></tr>`;
  }
}

// 🔄 Fetch data initially and then every 1 minute
fetchMarketData();
setInterval(fetchMarketData, 60000);
