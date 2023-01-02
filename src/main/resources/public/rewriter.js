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