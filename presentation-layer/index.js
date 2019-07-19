var query;

document.getElementById('form').onsubmit = function() { 
    query = document.getElementById('form-text').value;
    console.log(query);
    startLambda();
    return false;
};

// Create a request variable and assign a new XMLHttpRequest object to it.
var request = new XMLHttpRequest();
var BE_IP = 'INSERT REST-API SERVER IP HERE';

function startLambda() {
	let apiUrl = 'http://' + BE_IP + '/getMostRelevant/' + query;
	request.open('GET', apiUrl, true);
	request.send();
}

function getMRIFromQuery() {
	let apiUrl = 'http://' + BE_IP + '/getResults/' + query;
	request.open('GET', apiUrl, true);

	request.onload = function () {
		let data = JSON.parse(this.response);
		let imageCounter = 1;
		retrieveImagesFromPath(data);
	}

	request.send();
}

function retrieveImagesFromPath(paths) {
	console.log(paths);
	let count = 1;
	paths.forEach(path => {
		let apiUrl = 'http://' + BE_IP + '/getImage' + path.path;
		var request = new XMLHttpRequest();
		request.open('GET', apiUrl, true);
		request.responseType = 'blob';

		request.onload = function () {
			blob = this.response;
			var reader = new FileReader();
			reader.readAsDataURL(blob); 
			reader.onloadend = function() {
			 	base64data = reader.result;                
			 	document.getElementById("result-" + count.toString()).src = base64data;
			 	count++;
			}
		}

		request.send();
	});
}
