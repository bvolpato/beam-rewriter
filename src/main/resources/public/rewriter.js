console.info('Rewriter loaded');

var editor = ace.edit("editor");
editor.setTheme("ace/theme/xcode");
editor.session.setMode("ace/mode/java");

function loaderOn() {
  document.getElementById("loader").classList.add("loader");
}
function loaderOff() {
  document.getElementById("loader").classList.remove("loader");
}

function runCookbook() {
  loaderOn();

  var cookbook = document.getElementsByName("cookbook")[0].value;

  const xhr = new XMLHttpRequest();
  xhr.open("POST", '/convert', true);

  //Send the proper header information along with the request
  xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");

  xhr.onreadystatechange = () => { // Call a function when the state changes.
    if (xhr.readyState === XMLHttpRequest.DONE && xhr.status === 200) {
      editor.setValue(xhr.responseText, -1);
    }

    loaderOff();
  }
  xhr.send("cookbook=" + encodeURIComponent(cookbook) + "&code="
      + encodeURIComponent(editor.getValue()));

}

function convertProject() {
  loaderOn();

  var cookbook = document.getElementsByName("cookbook")[0].value;
  var file = document.getElementsByName("file")[0].files[0];

  const xhr = new XMLHttpRequest();
  xhr.open("POST", '/convertProject', true);
  xhr.responseType = "blob";

  xhr.onload = function (e) {
    if (xhr.readyState === XMLHttpRequest.DONE && xhr.status === 200) {
      console.info('response1112', xhr.readyState, xhr.status);

      var a = document.createElement('a');
      var type = xhr.getResponseHeader('Content-Type');
      var blob = new Blob([this.response], {type: type});

      a.href = URL.createObjectURL(blob);
      a.download = file.name;
      a.dispatchEvent(new MouseEvent('click'));
    }

    loaderOff();

  }

  var formData = new FormData();
  formData.append("cookbook", cookbook);
  formData.append("file", file);

  xhr.send(formData);

}