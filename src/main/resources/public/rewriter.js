console.info('Rewriter loaded');

function runCookbook() {
  console.info('Cookbook!')
  var cookbook = document.getElementsByName("cookbook")[0].value;
  var code = document.getElementsByName("code")[0].value;

  const xhr = new XMLHttpRequest();
  xhr.open("POST", '/convert', true);

  //Send the proper header information along with the request
  xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");

  xhr.onreadystatechange = () => { // Call a function when the state changes.
    if (xhr.readyState === XMLHttpRequest.DONE && xhr.status === 200) {
      document.getElementsByName("code")[0].value = xhr.responseText;
    }
  }
  xhr.send("cookbook=" + encodeURIComponent(cookbook) + "&code="
      + encodeURIComponent(code));

}


function convertProject() {
  console.info('Cookbook Project!')
  var cookbook = document.getElementsByName("cookbook")[0].value;
  var file = document.getElementsByName("file")[0].files[0];

  const xhr = new XMLHttpRequest();
  xhr.open("POST", '/convertProject', true);

  xhr.onreadystatechange = () => { // Call a function when the state changes.
    if (xhr.readyState === XMLHttpRequest.DONE && xhr.status === 200) {
      console.info('response111', xhr.readyState, xhr.status);

      var blob = xhr.response;

      var a = document.createElement('a');
      var type = xhr.getResponseHeader('Content-Type');
      var blob = new Blob([xhr.response], { type: type });

      a.href = URL.createObjectURL(blob);
      a.download = file.name;
      a.dispatchEvent(new MouseEvent('click'));
    }
  }

  var formData = new FormData();
  formData.append("cookbook", cookbook);
  formData.append("file", file);

  xhr.send(formData);

}
