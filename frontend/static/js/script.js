window.addEventListener('DOMContentLoaded', function(ev){

    let sparkChart = null;
    const cmbOption = document.getElementById('cmbOption');
    const spinner = document.getElementById('loadingSpinner');

    cmbOption.addEventListener('change', async function(ev){
        if(ev.target.value === "X") return;

        if(sparkChart != null) sparkChart.destroy();

        cmbOption.disabled = true;
        spinner.style.display = "block";

        const launchReq = await fetch("ml/", {
            method: "POST",
            body: JSON.stringify({ option: ev.target.value }),
            headers: { "Content-Type": "application/json" }
        });
        const launchResp = await launchReq.json();
        const taskId = launchResp.task_id;

        const pollResult = async () => {
            const resReq = await fetch(`ml/result/${taskId}/`);
            const resJson = await resReq.json();

            if(resJson.status && resJson.status === "pending") {
                setTimeout(pollResult, 1000);
            } else {
                // Datos listos
                spinner.style.display = "none";
                cmbOption.disabled = false;

                let resp = resJson;
                let graphicTitle = "";
                let data = {
                    labels: resp.map(p => p.product_name ? p.product_name.substring(0,50) + "..." : p.color),
                    values: resp.map(p => p.final_price ?? p.count ?? 0),
                };
                let graphicType = "";
                let backgroundColors = generateHexColors(10);
                console.log("|||||", ev.target.value)
                console.log("||||", data)
                switch(ev.target.value){
                    case "on_stock":
                        graphicTitle = "Products without stock";
                        graphicType = "bar";
                        break;
                    case "over_10_usd":
                        graphicTitle = "Stock > 10";
                        graphicType = "doughnut";
                        break;
                    case "most_popular_color":
                        graphicTitle = "# of units per most popular colors";
                        graphicType = "bar";
                        data = {
                            labels: resp.map(p => p.color),
                            values: resp.map(p => p.count),
                        };
                        break;
                    case "most_expensive":
                        graphicTitle = "Most expensive colors";
                        graphicType = "bar";
                        break;
                    default:
                        return;
                }

                const ctx = document.getElementById('sparkChart').getContext('2d');
                sparkChart = new Chart(ctx, {
                    type: graphicType,
                    data: {
                        labels: data.labels,
                        datasets: [{
                            label: graphicTitle,
                            data: data.values,
                            backgroundColor: backgroundColors,
                            borderColor: 'rgba(255, 99, 132, 1)',
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: false,
                        maintainAspectRatio: false
                    }
                });
            }
        };

        pollResult();
    });

function generateHexColors(n) {
  const colors = [];

  for (let i = 0; i < n; i++) {
    const color = '#' + Math.floor(Math.random() * 0xffffff)
      .toString(16)
      .padStart(6, '0');
    colors.push(color);
  }

  return colors;
}


});
