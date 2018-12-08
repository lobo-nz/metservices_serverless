const endpoints = {
  direction: 'https://59kkf4hheb.execute-api.ap-southeast-2.amazonaws.com/prod/direction/',
  speed : 'https://59kkf4hheb.execute-api.ap-southeast-2.amazonaws.com/prod/speed/',
  waves: 'https://59kkf4hheb.execute-api.ap-southeast-2.amazonaws.com/prod/height/',
  temperature: 'https://59kkf4hheb.execute-api.ap-southeast-2.amazonaws.com/prod/temperature/',
  deleteAll: 'https://59kkf4hheb.execute-api.ap-southeast-2.amazonaws.com/prod/deleteall/'
}

function generateDirectionChart(endpoint, options) {

  axios.get(endpoint).then(function(response) {
    const seaSurface = 'sea_surface_wave_from_direction_at_variance_spectral_density_maximum',
      windDirection = 'wind_from_direction_at_10m_above_ground_level';

    const labels = response.data.data.filter(function(element){
      //console.log(element[seaSurface])

      return element[seaSurface] !== "" || element[seaSurface] !== "0"
    }).map(function(surface){
      return surface.datetime;
    });

    //console.log(labels);

    const windDirections = response.data.data.map(function(elem){
      return elem[windDirection];
    });

    const seaSurfaces = response.data.data.map(function(surf){
      return surf[seaSurface];
    })

  const ctx = document.getElementById('myChart');
  const data = {
    labels: labels,
    datasets: [{
        data: windDirections,
        label: "Wind from direction at 10m above ground level",
        borderColor: "#3e95cd",
        fill: false
      },
      {
        data: seaSurfaces,
        label: "Sea surface wave from direction at variance spectral density maximum",
        borderColor: "#f44253",
        fill: false
      }
    ]
  }

  return generateChart(ctx, data, 'Wind and Sea direction');
  })
}

function generateSpeedChart (endpoint) {
  const windSpeed = 'wind_speed_at_10m_above_ground_level';
  const waterSpeed = 'surface_sea_water_speed';

  axios.get(endpoint).then(function(response) {
    const labels = response.data.data.filter(function(element){
      return element[windSpeed] !== ""
    }).map(function(surface){
      return surface.datetime;
    });

    const windSpeeds = response.data.data.map(function(speed){
      return speed[windSpeed];
    })

    const waterSpeeds = response.data.data.map(function(speed){
      return speed[waterSpeed];
    })

    //console.log(windSpeeds);

    const data = {
      labels: labels,
      datasets: [
        {
          data: windSpeeds,
          label: "Wind speed at 10m above ground level",
          borderColor: '#42f46b',
          fill: false
        },
        {
          data: waterSpeeds,
          label: "Surface sea water speed",
          borderColor: '#e83e55',
          fill: false
        }
      ]
    }
  const ctx = document.getElementById('myChart2');
  return generateChart(ctx, data, 'Wind and Water Sea Speed');
  });

}


function generateHeightChart(endpoint) {
  const significantHeight = 'sea_surface_wave_significant_height';
  const maximumHeight = 'sea_surface_wave_maximum_height';

  axios.get(endpoint).then(function(response) {
    const labels = response.data.data.filter(function(element){
      return element[significantHeight] !== ""
    }).map(function(surface){
      return surface.datetime;
    });

    const significantHeights = response.data.data.map(function(speed){
      return speed[significantHeight];
    })

    const maximumHeights = response.data.data.map(function(speed){
      return speed[maximumHeight];
    })

    //console.log(significantHeights);

    const data = {
      labels: labels,
      datasets: [
        {
          data: significantHeights,
          label: "significant height",
          borderColor: '#42f46b',
          fill: false
        },
        {
          data: maximumHeights,
          label: "maximum height",
          borderColor: '#e83e55',
          fill: false
        }
      ]
    }
  const ctx = document.getElementById('myChart3');
  return generateChart(ctx, data, 'Waves Height');
  });

}

function generateTemperatureChart(endpoint) {
  const airTempAboveGroundLevel = 'air_temperature_at_2m_above_ground_level';
  //const maximumHeight = 'sea_surface_wave_maximum_height';

  axios.get(endpoint).then(function(response) {
    const labels = response.data.data.filter(function(element){
      return element[airTempAboveGroundLevel] !== ""
    }).map(function(surface){
      return surface.datetime;
    });

    const airTempAboveGroundLevels = response.data.data.map(function(speed){
      return speed[airTempAboveGroundLevel];
    })

    const data = {
      labels: labels,
      datasets: [
        {
          data: airTempAboveGroundLevels,
          label: "Air Temperature At 2m Above The Ground Level",
          borderColor: '#42f46b',
          fill: false
        }
      ]
    }
  const ctx = document.getElementById('myChart4');
  return generateChart(ctx, data, 'Temperature');
  });

}

generateDirectionChart(endpoints.direction)
generateSpeedChart(endpoints.speed)
generateHeightChart(endpoints.waves)
generateTemperatureChart(endpoints.temperature)

function generateChart(context, data, text) {
  return new Chart(context, {
    type: 'line',
    data: data,
    options: {
      title: {
        display: true,
        text: text
      }
    }
  })
}


var metServicesBucketName = 'met-service-assesment';
var bucketRegion = 'ap-southeast-2';
var IdentityPoolId = 'ap-southeast-2:628dabe4-6645-4c33-a4b0-7b486b98fb83';

AWS.config.update({
  region: bucketRegion,
  credentials: new AWS.CognitoIdentityCredentials({
    IdentityPoolId: IdentityPoolId
  })
});

var s3 = new AWS.S3({
  apiVersion: '2006-03-01',
  params: {Bucket: metServicesBucketName}
});

function addCSVFile() {
  var files = document.getElementById('csvupload').files;
  if (!files.length) {
      return alert('Please choose a file to upload first.');
  }

  var file = files[0];
  var fileName = file.name;
  var csvPath =  'csvfiles/';

  var csvKey = csvPath + fileName;
  s3.upload({
      Key: csvKey,
      Body: file,
      ACL: 'public-read'
    }, function(err, data) {
      if (err) {
        return alert('There was an error uploading your file: ', err.message);
      }
      alert('Successfully uploaded file.');
});
}

function addJsonFile() {
  var files = document.getElementById('jsonupload').files;
  if (!files.length) {
      return alert('Please choose a file to upload first.');
  }

  var file = files[0];
  var fileName = file.name;
  var jsonPath =  'jsonfiles/';

  var jsonKey = jsonPath + fileName;
  s3.upload({
      Key: jsonKey,
      Body: file,
      ACL: 'public-read'
    }, function(err, data) {
      if (err) {
        return alert('There was an error uploading your file: ', err.message);
      }
      alert('Successfully uploaded file.');
});
}

function deleteAllItems() {

  var r = confirm("Are you sure you want to delete all data items in all tables?");
  if (r == true) {
    axios.get(endpoints.deleteAll).then(data=>console.log(data)).catch(err=>console.log(err));
  }
}
