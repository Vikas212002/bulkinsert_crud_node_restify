const moment = require('moment');

function randomTime(start, end) {
    var diff = end.getTime() - start.getTime();
    var new_diff = diff * Math.random();
    var date = new Date(start.getTime() + new_diff);
    return date;
}

var today = new Date();
var tomorrow = new Date(today);
tomorrow.setDate(today.getDate() + 1);

var start = new Date(today);
start.setHours(8, 0, 0, 0);

var end = new Date(tomorrow);
end.setHours(18, 0, 0, 0);

var randomDate = randomTime(start, end);
console.log(randomDate);

var randomDate = randomTime(start, end);
var formattedDate = moment(randomDate).format('YYYY-MM-DD HH:mm:ss');
console.log(formattedDate);