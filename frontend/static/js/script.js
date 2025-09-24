window.addEventListener('DOMContentLoaded', function(ev){

    let sparkChart = null;
    const cmbOption = document.getElementById('cmbOption');
    cmbOption.addEventListener('change', async function(ev){
        if(sparkChart != null) sparkChart.destroy();
        const req = await fetch("ml/", {
            method: "POST",
            body: JSON.stringify({ option: ev.target.value }),
        })
        const resp = await req.json();
        console.log(resp);
        let graphicTitle = "";
         data =  {
            labels: resp.map( p => p.product_name.substring(0,10)),
            values: resp.map( p => p.final_price),
        }
        let graphicType = "";
        let backgroundColors = ['#FF6384', '#36A2EB', '#FFCE56'];
        switch(ev.target.value){
            case "no_stock":
            graphicTitle = "Products without stock";
            graphicType = "bar";
            break;
            case "over 10usd":
            graphicTitle = "Stock > 10";
            graphicType = "doughnut";
            break;
            case "most popular color":
            graphicTitle = "# of units per  most popular colors";
            graphicType  ="bar";
            data =  {
                labels: resp.map( p => p.product_name.substring(0,10)),
                values: resp.map( p => p.count),
            }

            break;
            case "most expensive":
            graphicTitle = "Most expensive colors";
            graphicType = "bar"
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
    })
})