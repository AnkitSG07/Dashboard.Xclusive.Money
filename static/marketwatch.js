async function fetchMarketData() {
  try {
    // Fetch Gainers
    const gainersResponse = await fetch('https://www.nseindia.com/api/live-analysis-variations?index=gainers', {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      }
    });

    const losersResponse = await fetch('https://www.nseindia.com/api/live-analysis-variations?index=losers', {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      }
    });

    const gainersData = await gainersResponse.json();
    const losersData = await losersResponse.json();

    updateTable('gainersTableBody', gainersData.data || []);
    updateTable('losersTableBody', losersData.data || []);

  } catch (error) {
    console.error("Market Watch Error:", error);
    document.getElementById('gainersTableBody').innerHTML = `<tr><td colspan="3" class="text-center text-muted">Error fetching data</td></tr>`;
    document.getElementById('losersTableBody').innerHTML = `<tr><td colspan="3" class="text-center text-muted">Error fetching data</td></tr>`;
  }
}

function updateTable(tableId, stocks) {
  const tableBody = document.getElementById(tableId);
  tableBody.innerHTML = "";

  if (stocks.length > 0) {
    stocks.slice(0, 10).forEach(stock => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td><b>${stock.symbol || stock.data.symbol}</b></td>
        <td>${stock.lastPrice || stock.data.lastPrice}</td>
        <td class="${parseFloat(stock.pChange || stock.data.pChange) > 0 ? 'text-success' : 'text-danger'}">
          ${parseFloat(stock.pChange || stock.data.pChange).toFixed(2)}%
        </td>
      `;
      tableBody.appendChild(row);
    });
  } else {
    tableBody.innerHTML = `<tr><td colspan="3" class="text-center text-muted">No data available</td></tr>`;
  }
}

// Initial load + Auto refresh every 1 minute
fetchMarketData();
setInterval(fetchMarketData, 60000);
