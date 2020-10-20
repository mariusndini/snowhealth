var global_diet={};
var gweight_measure=" g"
var cal_measure=" cal";
var global_empty=[{"ID":"","DATE":"","ACTIVE_ENERGY_BURNED":-1,"APPLE_STAND_TIME":-1,"BASAL_ENERGY_BURNED":-1,"CARBS":-1,"CHOLESTEROL":-1,"DIETARY_ENERGY":-1,"FATMONO":null,"FATPOLY":null,"FATSAT":-1,"FATTOTAL":-1,"FLIGHTSCLIMBED":-1,"PROTEIN":-1,"SODIUM":-1,"STEPS":-1,"SUGAR":-1,"WALK_RUN": -1}];
var empty_chart=[{"DATE":"2020-04-22 00:00:00.000","val":""},{"DATE":"2020-04-22 01:00:00.000","val":"0"},{"DATE":"2020-04-22 02:00:00.000","val":"0"},{"DATE":"2020-04-22 03:00:00.000","val":"0"},{"DATE":"2020-04-22 04:00:00.000","val":"0"},{"DATE":"2020-04-22 05:00:00.000","val":"0"},{"DATE":"2020-04-22 06:00:00.000","val":"0"},{"DATE":"2020-04-22 07:00:00.000","val":"0"},{"DATE":"2020-04-22 08:00:00.000","val":"0"},{"DATE":"2020-04-22 09:00:00.000","val":"0"},{"DATE":"2020-04-22 10:00:00.000","val":"0"},{"DATE":"2020-04-22 11:00:00.000","val":"0"},{"DATE":"2020-04-22 12:00:00.000","val":"0"},{"DATE":"2020-04-22 13:00:00.000","val":"0"},{"DATE":"2020-04-22 14:00:00.000","val":"0"},{"DATE":"2020-04-22 15:00:00.000","val":"0"},{"DATE":"2020-04-22 16:00:00.000","val":"0"},{"DATE":"2020-04-22 17:00:00.000","val":"0"},{"DATE":"2020-04-22 18:00:00.000","val":"0"},{"DATE":"2020-04-22 19:00:00.000","val":"0"},{"DATE":"2020-04-22 20:00:00.000","val":"0"},{"DATE":"2020-04-22 21:00:00.000","val":"0"},{"DATE":"2020-04-22 22:00:00.000","val":"0"},{"DATE":"2020-04-22 23:00:00.000","val":"0"}];
var charts={};
downloadCSVFromJson = (filename, arrayOfJson, columns) => {
  // convert JSON to CSV
  const replacer = (key, value) => value === null ? '' : value // specify how you want to handle null values here
  //const header = Object.keys(arrayOfJson[0])
  let csv = arrayOfJson.map(row => columns.map(fieldName => 
  JSON.stringify(row[fieldName], replacer)).join(','))
  csv.unshift(columns.join(','))
  csv = csv.join('\r\n')

  // Create link and download
  var link = document.createElement('a');
  link.setAttribute('href', 'data:text/csv;charset=utf-8,%EF%BB%BF' + encodeURIComponent(csv));
  link.setAttribute('download', filename);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}
function getCheckedBoxes(chkboxName) {
  var checkboxes = document.getElementsByName(chkboxName);
  var checkboxesChecked = [];
  // loop over them all
  for (var i=0; i<checkboxes.length; i++) {
     // And stick the checked ones onto an array...
     if (checkboxes[i].checked) {
        checkboxesChecked.push(checkboxes[i].value);
     }
  }
  // Return the array if it is non-empty, or null
  return checkboxesChecked.length > 0 ? checkboxesChecked : null;
}
function refresh_data(data){
    global_diet=data[0];
    //downloadCSVFromJson("test.csv",data);
    if(Number(global_diet.PROTEIN)>=0){
        $('#prot_gr').html(Number(global_diet.PROTEIN).toFixed(0)+gweight_measure);
        $('#prot_cal').html(Number(global_diet.PROTEIN*4).toFixed(0)+cal_measure);
    }else{
        $('#prot_gr').html("N/A");
        $('#prot_cal').html("N/A");
    }
    if(Number(global_diet.CARBS)>=0){
        $('#carb_gr').html(Number(global_diet.CARBS).toFixed(0)+gweight_measure);
        $('#carb_cal').html(Number(global_diet.CARBS*4).toFixed(0)+cal_measure);
    }else{
        $('#carb_gr').html("N/A");
        $('#carb_cal').html("N/A");
    }
    if(Number(global_diet.FATTOTAL)>=0){
        $('#fat_gr').html(Number(global_diet.FATTOTAL).toFixed(0)+gweight_measure);
        $('#fat_cal').html(Number(global_diet.FATTOTAL*9).toFixed(0)+cal_measure);
        $('#fattotal_gr').html(Number(global_diet.FATTOTAL).toFixed(0)+gweight_measure);
        $('#fattotal_cal').html(Number(global_diet.FATTOTAL*9).toFixed(0)+cal_measure);
    }else{
        $('#fat_gr').html("N/A");
        $('#fat_cal').html("N/A");
        $('#fattotal_gr').html("N/A");
        $('#fattotal_cal').html("N/A");
    }
    if(Number(global_diet.SODIUM)>=0){
        $('#sodium_gr').html(Number(global_diet.SODIUM).toFixed(0)+gweight_measure);
    }else{
        $('#sodium_gr').html("N/A");
    }
    if(Number(global_diet.CHOLESTEROL)>=0){
        $('#chol_gr').html(Number(global_diet.CHOLESTEROL).toFixed(0)+gweight_measure);
    }else{
        $('#chol_gr').html("N/A");
    }
    if(Number(global_diet.SUGAR)>=0){
        $('#sug_gr').html(Number(global_diet.SUGAR).toFixed(0)+gweight_measure);
    }else{
        $('#sug_gr').html("N/A");
    }
    if(Number(global_diet.FATSAT)>=0){
        $('#fatsat_gr').html(Number(global_diet.FATSAT).toFixed(0)+gweight_measure);
        $('#fatsat_cal').html(Number(global_diet.FATSAT*9).toFixed(0)+cal_measure);
    }else{
        $('#fatsat_gr').html("N/A");
        $('#fatsat_cal').html("N/A");
    }
    if(Number(global_diet.FATMONO)>=0){
        $('#fatmono_gr').html(Number(global_diet.FATMONO).toFixed(0)+gweight_measure);
        $('#fatmono_cal').html(Number(global_diet.FATMONO*9).toFixed(0)+cal_measure);
    }else{
        $('#fatsat_gr').html("N/A");
        $('#fatsat_cal').html("N/A");
    }
    if(Number(global_diet.FATPOLY)>=0){
        $('#fatpoly_gr').html(Number(global_diet.FATPOLY).toFixed(0)+gweight_measure);
        $('#fatpoly_cal').html(Number(global_diet.FATPOLY*9).toFixed(0)+cal_measure);
    }else{
        $('#fatpoly_gr').html("N/A");
        $('#fatpoly_cal').html("N/A");
    }
    $('#total_gr').html(Number(global_diet.FATTOTAL+global_diet.CARBS+global_diet.PROTEIN).toFixed(0)+gweight_measure);
    $('#total_cal').html(Number((global_diet.FATTOTAL*9)+(global_diet.CARBS*4)+(global_diet.PROTEIN*4)).toFixed(0)+cal_measure);    
    var totalgr=Number(global_diet.FATTOTAL+global_diet.CARBS+global_diet.PROTEIN);
    var values=[(global_diet.PROTEIN*4),(global_diet.FATTOTAL*9),(global_diet.CARBS*4)];
    if(global_diet.PROTEIN>0 && global_diet.CARBS>0 && global_diet.FATTOTAL>0){
        calories_chart(values);
    }else{
        document.getElementById("prime_chart").style.display="none";
        document.getElementById("calories_chart").style.display="none";
    }
    if(global_diet.HEART_RATE_ARR){
        if(global_diet.HEART_RATE_ARR.length>0){
            $('#hchart_msg').html("");
            heart_rate_chart(global_diet.HEART_RATE_ARR,"heart_rate_chart","Heart Rate");
        }else{
            //$('#hchart_msg').html("No Data");
            heart_rate_chart(empty_chart,"heart_rate_chart","Heart Rate - No Data");
        }
    }else{
        //$('#hchart_msg').html("No Data");
        heart_rate_chart(empty_chart,"heart_rate_chart","Heart Rate - No Data");
    }
    if(global_diet.REST_HEART_RATE_ARR){
        if(global_diet.HEART_RATE_ARR.length>0){
            $('#hchart_msg1').html("");
            heart_rate_chart(global_diet.REST_HEART_RATE_ARR,"rest_heart_rate_chart","Rest Heart Rate");
        }else{
            //$('#hchart_msg1').html("No Data");
            heart_rate_chart(empty_chart,"rest_heart_rate_chart","Rest Heart Rate - No Data");
        }
    }else{
        //$('#hchart_msg1').html("No Data");
        heart_rate_chart(empty_chart,"rest_heart_rate_chart","Rest Heart Rate - No Data");
    }
    if(global_diet.SDNN_HEART_RATE_ARR){
        if(global_diet.HEART_RATE_ARR.length>0){
            $('#hchart_msg2').html("");
            heart_rate_chart(global_diet.SDNN_HEART_RATE_ARR,"sdnn_heart_rate_chart","SDNN Heart Rate");
        }else{
            //$('#hchart_msg2').html("No Data");
            heart_rate_chart(empty_chart,"sdnn_heart_rate_chart","SDNN Heart Rate - No Data");
        }
    }else{
        //$('#hchart_msg2').html("No Data");
        heart_rate_chart(empty_chart,"sdnn_heart_rate_chart","SDNN Heart Rate - No Data");
    }
    if(global_diet.WALK_HEART_RATE_ARR){
        if(global_diet.WALK_HEART_RATE_ARR.length>0){
            $('#hchart_msg3').html("");
            heart_rate_chart(global_diet.WALK_HEART_RATE_ARR,"walk_heart_rate_chart","Walk/Run Heart Rate");
        }else{
            //$('#hchart_msg3').html("No Data");
            heart_rate_chart(empty_chart,"walk_heart_rate_chart","Walk/Run Heart Rate - No Data");
        }
    }else{
        //$('#hchart_msg3').html("No Data");
        heart_rate_chart(empty_chart,"walk_heart_rate_chart","Walk/Run Heart Rate - No Data");
    }
    if(global_diet.ENVIRONMENT_AUDIO_ARR){
        if(global_diet.ENVIRONMENT_AUDIO_ARR.length>0){
            $('#env_msg').html("");
            heart_rate_chart(global_diet.ENVIRONMENT_AUDIO_ARR,"env_chart","Environment Audio");
        }else{
            //$('#env_msg').html("No Data");
            heart_rate_chart(empty_chart,"env_chart","Environment Audio - No Data");
        }
    }else{
        //$('#env_msg').html("No Data");
        heart_rate_chart(empty_chart,"env_chart","Environment Audio - No Data");
    }
    if(global_diet.HEADPHONE_AUDIO_ARR){
        if(global_diet.HEADPHONE_AUDIO_ARR.length>0){
            $('#headphone_msg').html("");
            heart_rate_chart(global_diet.HEADPHONE_AUDIO_ARR,"headphone_chart","Headphone Audio");
        }else{
            //$('#headphone_msg').html("No Data");
            heart_rate_chart(empty_chart,"headphone_chart","Headphone Audio - No Data");
        }
    }else{
        heart_rate_chart(empty_chart,"headphone_chart","Headphone Audio - No Data");
    }
    if(global_diet.STEPS_ARR){
        if(global_diet.STEPS_ARR.length>0){
            $('#steps_msg').html("");
            draw_bar_chart(global_diet.STEPS_ARR,"steps_chart","Steps");
        }else{
            //$('#steps_msg').html("No Data");
            draw_bar_chart(empty_chart,"steps_chart","Steps - No Data");
        }
    }else{
        //$('#steps_msg').html("No Data");
        draw_bar_chart(empty_chart,"steps_chart","Steps - No Data");
    }
    if(global_diet.ACTIVE_ENERGY_ARR){
        if(global_diet.ACTIVE_ENERGY_ARR.length>0){
            $('#active_en_msg').html("");
            draw_bar_chart(global_diet.ACTIVE_ENERGY_ARR,"active_en_chart","Active Energy");
        }else{
            //$('#active_en_msg').html("No Data");
            draw_bar_chart(empty_chart,"active_en_chart","Active Energy - No Data");
        }
    }else{
       //$('#active_en_msg').html("No Data");
        draw_bar_chart(empty_chart,"active_en_chart","Active Energy - No Data");
    }
    if(global_diet.BASAL_ARR){
        if(global_diet.BASAL_ARR.length>0){
            $('#basal_en_msg').html("");
            draw_bar_chart(global_diet.BASAL_ARR,"basal_en_chart","Basal Energy");
        }else{
          //$('#basal_en_msg').html("No Data");
            draw_bar_chart(empty_chart,"basal_en_chart","Basal Energy - No Data");
        }
    }else{
       //$('#basal_en_msg').html("No Data");
        draw_bar_chart(empty_chart,"basal_en_chart","Basal Energy - No Data");
    }
    if(global_diet.STAND_TIME_ARR){
        if(global_diet.STAND_TIME_ARR.length>0){
            $('#walkrun_msg').html("");
            draw_bar_chart(global_diet.STAND_TIME_ARR,"walkrun_chart","Walk/Run");
        }else{
          //$('#walkrun_msg').html("No Data");
            draw_bar_chart(empty_chart,"walkrun_chart","Walk/Run - No Data");
        }
    }else{
       //$('#walkrun_msg').html("No Data");
        draw_bar_chart(empty_chart,"walkrun_chart","Walk/Run - No Data");
    }
    if(global_diet.FLIGHTS_ARR){
        if(global_diet.FLIGHTS_ARR.length>0){
            $('#flightsclimbed_msg').html("");
            draw_bar_chart(global_diet.FLIGHTS_ARR,"flightsclimbed_chart","Flights Climbed");
        }else{
        //    $('#flightsclimbed_msg').html("No Data");
            draw_bar_chart(empty_chart,"flightsclimbed_chart","Flights Climbed - No Data");
        }
    }else{
       // $('#flightsclimbed_msg').html("No Data");
        draw_bar_chart(empty_chart,"flightsclimbed_chart","Flights Climbed - No Data");
    }
    $('#total_burned_cal').html((Number(global_diet.ACTIVE_ENERGY_BURNED)+Number(global_diet.BASAL_ENERGY_BURNED)).toFixed(0)+cal_measure);
    $('#energyburned').html(Number(global_diet.ACTIVE_ENERGY_BURNED).toFixed(0)+cal_measure);
    $('#basalenergyburned').html(Number(global_diet.BASAL_ENERGY_BURNED).toFixed(0)+cal_measure);
    $('#applestands').html(Number(global_diet.APPLE_STAND_TIME).toFixed(0)+" s");
    $('#flightsclimbed').html(Number(global_diet.FLIGHTSCLIMBED).toFixed(0));
    $('#stepscount').html(Number(global_diet.STEPS).toFixed(0));
    $('#walkrun_meters').html(Number(global_diet.WALK_RUN).toFixed(0)+" m");
    $('#age').html(global_diet.AGE);
    if(Number(global_diet.AGE)>=0){
       $('#age').html(Number(global_diet.AGE)); 
    }else{
         $('#age').html("N/A"); 
    }
    if (global_diet.BLOODTYPE==null || global_diet.BLOODTYPE=="Unknown" || global_diet.BLOODTYPE=="") {
        $('#bloodtype').html("N/A");
    }else{
        $('#bloodtype').html(global_diet.BLOODTYPE);
    }
    if (global_diet.GENDER=="Male") {
        $('#gender').html(`<i style="font-size: 35px; color:#1474be" class='fas fa-mars' aria-hidden="true"></i>`+" "+global_diet.GENDER);
    }else if (!global_diet.GENDER) {
        $('#gender').html("N/A");
    }else{
        $('#gender').html(`<i style="font-size: 35px; color:#FF82AB" class='fas fa-venus' aria-hidden="true"></i>`+" "+global_diet.GENDER);
    }
}
function draw_heartrate(values){
            var layout = {
                xaxis:{
                    color: "azure"
                },
                yaxis:{
                    color: "azure"
                },
                legend: {
                    bgcolor: "#0d2132",
                    font: {
                        color: "azure",
                    }
                },
                height: 300,
               // plot_bgcolor: "#ff0000"
            };
            var config = {
                responsive: true,
                displayModeBar: false
            };
var data = [{
       title: {
      text:"<b style='color: azure;'>Calories</b>",
      font: { size: 11 }
    },
    name: "Heart Rate",
  type: "scatter",
  mode: "markers+text",
  x: unpack(values, 'DATE'),
  y: unpack(values, 'val'),
  line: {color: '#17BECF'}
}
];

            Plotly.newPlot('heart_chart', data, layout, config);
}

function draw_piechart(values){
            var layout = {
                xaxis:{
                    color: "azure"
                },
                yaxis:{
                    color: "azure"
                },
                legend: {
                    bgcolor: "#0d2132",
                    font: {
                        color: "azure",
                    }
                },
                height: 200,
               // plot_bgcolor: "#ff0000"
            };
            var config = {
                responsive: true,
                displayModeBar: false
            }
var data = [
  {
    type: "indicator",
    mode: "number+gauge+delta",
    value: values[0]+values[1]+values[2],
    domain: { x: [0, 1], y: [0, 1] },
    delta: { 
        reference: 2000,
        position: "top",
        increasing: {
            symbol: "▲",
            color:"#FF4136"
        },
        decreasing: {
            symbol: "▼",
            color:"#3D9970"
        },
        font: {
            size: 10
        } 
    },
    title: {
      text:"<b style='color: azure;'>Calories</b>",
      font: { size: 11 }
    },
    number:{
      font:{
        color: "azure",
        size: 14
      }
    },
    gauge: {
      shape: "bullet",
      axis: { 
        range: [null, 2000],
        tickfont:{color: "azure"}
       },
      threshold: {
        line: { color: "red", width: 2, gradient: { yanchor: "vertical" } },
        thickness: 2,
        value: 2000
      },
      bordercolor: "azure",
      bgcolor: "#0d2132",
      color: "azure",
    //  steps: [{ range: [0, values[0]], color: "#ff6961" },{ range: [values[0], values[1]], color: "green" },{ range: [values[1], values[2]], color: "cyan" }],
      bar: { color: "#1474be" }
    }
  }
];

            Plotly.newPlot('prime_chart', data, layout, config);
}
function execute_select_query(sqlquery,columns){
             $.ajax({
                        "async": true,
                        "crossDomain": true,
                        "url": "https://jkduwmfkfb.execute-api.us-east-1.amazonaws.com/dev/send",
                        "method": "POST",
                        data: `{"body":{"select":` + sqlquery + `}}`,
                        beforeSend: modal,
                        success: function(result) {
                            console.log(result);
                            setTimeout(function(){
                            var id = $("#pininput").val();
                            if(result!=undefined){
                                if(result.body.length>0){
                                    downloadCSVFromJson(id+"_CSV.csv",result.body,columns);
                                    $('#downloadDialog').modal('hide');
                                }else{
                                    alert("Data Not Found");
                                }
                            }else{
                                alert("Data Not Found");
                            }
                            },100)
                             $('#busyDialog').modal('hide');
                            
                        },
                        error: function(error){
                            console.log(error);
                            $('#busyDialog').modal('hide');
                            $('#downloadDialog').modal('hide');
                        }
                    });
}
function get_data(){
                   var id = $("#pininput").val();
                var datestr,sql,isoDate;
            if (picked_date!=undefined) {
                isoDate = new Date(picked_date.getTime() - (picked_date.getTimezoneOffset() * 60000)).toISOString();
                datestr = isoDate.split("T")[0];
                sql = `"select * from HEALTHKIT.HK.POP_AGG where id ='` + id + `' and date='` + datestr + `'  order by date desc;"`
            } else {
                sql = `"select * from HEALTHKIT.HK.POP_AGG where id ='` + id + `' order by date desc;"`
            }
             $.ajax({
                        "async": true,
                        "crossDomain": true,
                        "url": "https://jkduwmfkfb.execute-api.us-east-1.amazonaws.com/dev/send",
                        "method": "POST",
                        data: `{"body":{"select":` + sql + `}}`,
                        beforeSend: modal,
                        success: function(result) {
                            setTimeout(function(){
                            refresh_charts();
                            if(result!=undefined){
                                if(result.body.length>0){
                                    refresh_data(result.body);
                                }else{
                                    alert("Data Not Found");
                                    refresh_data(global_empty);
                                }
                            }else{
                                alert("Data Not Found");
                                refresh_data(global_empty);
                            }
                            },100)
                             $('#busyDialog').modal('hide');
                            
                        },
                        error: function(error){
                            console.log(error);
                            $('#busyDialog').modal('hide');
                        }
                    });
}
function modal(){
    $('#busyDialog').modal('show');
}
  function unpack(rows, key) {
  return rows.map(function(row) { return row[key]; });
}
function calories_chart(values){
    var ctx = document.getElementById('calories_chart').getContext('2d');
    document.getElementById('calories_chart').style.display="block";
    document.getElementById('prime_chart').style.display="block";
    charts["calories_chart"] = new Chart(ctx, {
    type: 'horizontalBar',
    data: {
    labels:["Calories"],
    datasets: [
    {
        label:"Proteins",
        borderColor: "#1474be",
        backgroundColor:"#ff6961",
        barThickness: 25,
        maxBarThickness: 25,
        stack:"Calories",
        data: [values[0]],
    },
    {
        label:"Fats",
        borderColor: "#1474be",
        backgroundColor:"#fff096",
        barThickness: 25,
        maxBarThickness: 25,
        stack:"Calories",
        data: [values[1]],
    },
    {
        label:"Carbs",
        borderColor: "#1474be",
        backgroundColor:"#77dd77",
        barThickness: 25,
        maxBarThickness: 25,
        stack:"Calories",
        data: [values[2]],
    }
    ]},
     options: {
                legend:{
                        display: true,
            labels:{
                fontColor:"azure"
            }
        },
        scales: {
            xAxes: [{
              gridLines:{
                    color:"#283e53"
                },
                             ticks:{
        fontColor:"azure"
    },
                stacked: true
            }],
            yAxes: [{
                              gridLines:{
                    color:"#283e53"
                },
                             ticks:{
        fontColor:"azure"
    },
                stacked: true
            }]
        }
    }
});
}
function parseISO(s) {
  var b = s.split(/\D/);
  return new Date(b[0], b[1]-1, b[2], b[3], b[4], b[5]);
}
function refresh_charts(){
    document.getElementById("calories_chart").style.display="none";
    document.getElementById("prime_chart").style.display="none";
    document.getElementById("heart_rate_chart").style.display="none";
    document.getElementById("rest_heart_rate_chart").style.display="none";
    document.getElementById("sdnn_heart_rate_chart").style.display="none";
    document.getElementById("walk_heart_rate_chart").style.display="none";
    document.getElementById("env_chart").style.display="none";
    document.getElementById("headphone_chart").style.display="none";
    document.getElementById("active_en_chart").style.display="none";
    document.getElementById("basal_en_chart").style.display="none";
    document.getElementById("steps_chart").style.display="none";
    document.getElementById("walkrun_chart").style.display="none";
    document.getElementById("flightsclimbed_chart").style.display="none";
    var canvas=document.getElementById("calories_chart");
    var canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["calories_chart"]) {
        charts["calories_chart"].destroy();
    }

    canvas=document.getElementById("heart_rate_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["heart_rate_chart"]) {
        charts["heart_rate_chart"].destroy();
    }

    canvas=document.getElementById("rest_heart_rate_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["rest_heart_rate_chart"]) {
        charts["rest_heart_rate_chart"].destroy();
    }

    canvas=document.getElementById("sdnn_heart_rate_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["sdnn_heart_rate_chart"]) {
        charts["sdnn_heart_rate_chart"].destroy();
    }

    canvas=document.getElementById("walk_heart_rate_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["walk_heart_rate_chart"]) {
        charts["walk_heart_rate_chart"].destroy();
    }

    canvas=document.getElementById("env_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["env_chart"]) {
        charts["env_chart"].destroy();
    }

    canvas=document.getElementById("headphone_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["headphone_chart"]) {
        charts["headphone_chart"].destroy();
    }

    canvas=document.getElementById("active_en_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["active_en_chart"]) {
        charts["active_en_chart"].destroy();
    }

    canvas=document.getElementById("basal_en_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["basal_en_chart"]) {
        charts["basal_en_chart"].destroy();
    }

    canvas=document.getElementById("steps_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["steps_chart"]) {
        charts["steps_chart"].destroy();
    }
    canvas=document.getElementById("flightsclimbed_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["flightsclimbed_chart"]) {
        charts["flightsclimbed_chart"].destroy();
    }
    canvas=document.getElementById("walkrun_chart");
    canvas_cntx=canvas.getContext('2d');
    canvas_cntx.setTransform(1, 0, 0, 1, 0, 0);
    canvas_cntx.clearRect(0, 0, canvas.width, canvas.height);
    canvas_cntx.restore();
    if (charts["walkrun_chart"]) {
        charts["walkrun_chart"].destroy();
    }
    
}
function heart_rate_chart(data,element_id,title){
var datarenamed=[];
var renamed={};
for (var i = data.length - 1; i >= 0; i--) {
    renamed={
        x:new Date(parseISO(data[i].DATE)),
        t:new Date(parseISO(data[i].DATE)),
        y:data[i].val
    }
    datarenamed.push(renamed);
}
var ctx = document.getElementById(element_id).getContext('2d');
document.getElementById(element_id).style.display="block";
charts[element_id] = new Chart(ctx, {
    type: 'scatter',
    data: {
        datasets: [{
            data:datarenamed,
            pointBackgroundColor: "azure"
        }]
    },
    options: {
        legend:{
            display:false
        },
        title:{
            fontColor:"azure",
            display:true,
            padding: 15,
            text:title
        },
        scales: {
    yAxes:[{
        gridLines:{
                    color:"#283e53"
                },
                       ticks:{
        fontColor:"azure"
    },
    }],
            xAxes: [{
                   gridLines:{
                    color:"#283e53"
                },
                type: 'time',
                    ticks:{
        fontColor:"azure"
    },
                time: {
                   // unit: 'hour'
                },
                distribution: 'series',

            }]
        }
    }
});
}
function draw_bar_chart(data,element_id,title){
var datarenamed=[];
var renamed={};
for (var i = data.length - 1; i >= 0; i--) {
    renamed={
        x:new Date(parseISO(data[i].DATE)),
        t:new Date(parseISO(data[i].DATE)),
        y:Number(data[i].val)
    }
    datarenamed.push(renamed);
}
var ctx = document.getElementById(element_id).getContext('2d');
document.getElementById(element_id).style.display="block";
charts[element_id] = new Chart(ctx, {
    type: 'bar',
    data: {
        datasets: [
    {
        borderColor: "1474be",
        backgroundColor:"#1474be",
        data: datarenamed,
    },
        ]
    },
    options: {
        legend:{
            display:false
        },
        title:{
            fontColor:"azure",
            display:true,
            padding: 15,
            text:title
        },
        scales: {
    yAxes:[{
        gridLines:{
                    color:"#283e53"
                },
                       ticks:{
        fontColor:"azure"
    },
    }],
            xAxes: [{
                   gridLines:{
                    color:"#283e53"
                },
                type: 'time',
                    ticks:{
        fontColor:"azure"
    },
                time: {
                   // unit: 'hour'
                },
                distribution: 'series',

            }]
        }
    }
});
}