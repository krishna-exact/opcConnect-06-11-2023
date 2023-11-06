var exec = require('child_process').exec, child;
const fs = require("fs")
const path = require('path');
const zlib = require('zlib');
//const fetch = require('node-fetch');
//const https = require('follow-redirects').https;
const request = require('request');
const propertiesReader = require('properties-reader');
const properties = propertiesReader(__dirname + "\\config.properties");
const timeInterval = 5 * 60 * 1000;
const ziprecord = 'zipped.txt'


/******************Delete logs older than 7 days */
// const sevenDaysAgo = new Date();
// sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

// const deleteOldLogs = (path_for_logs) => {
//     console.log("delete logs called");
//   fs.readdir(path_for_logs, (err, files) => {
//     if (err) {
//       return console.error(`Unable to read the logs directory: ${err}`);
//     }

//     files.forEach((file) => {
//       const filePath = path.join(path_for_logs, file);

//       fs.stat(filePath, (err, stats) => {
//         if (err) {
//           return console.error(`Unable to get file stats: ${err}`);
//         }

//         if (stats.mtime < sevenDaysAgo) {
//           fs.unlink(filePath, (err) => {
//             if (err) {
//               return console.error(`Unable to delete old log file: ${err}`);
//             }
//             console.log(`Deleted old log file: ${file}`);
//           });
//         }
//       });
//     });
//   });
// };

const compressFile = (filename) => {
    return new Promise((resolve, reject) => {
        var gzip = zlib.createGzip();
        var r = fs.createReadStream(filename);
        var w = fs.createWriteStream(filename + '.gz');
        r.pipe(gzip).pipe(w).on('finish', function (err) {
            if (err) {
                console.log(err);
                reject(err);
            } else {
                console.log("Zipped File!", filename)
                fs.appendFileSync(ziprecord, filename + '.gz' + '\n');
                console.log("Deleted File!",filename)
                fs.unlinkSync(filename)
                resolve(filename)
            }
        });

    })
}

async function removeUnwantedLogs() {

    const logs = fs.readdirSync(path_for_logs).filter(file => file.endsWith('.gz')).map(file => `${path_for_logs}\\${file}`);
    request.get(`${properties.get('CONFIG_URL_PREFIX')}/ingestconfigs`, (err, res, body) => {
        if (err) {
            console.error(`Error retrieving ingestconfigs: ${err}`);
            return;
        }
        if (res.statusCode !== 200) {
            console.error(`Error retrieving ingestconfigs, status code: ${res.statusCode}`);
            return;
        }
        const data = JSON.parse(body);
        const idlist = new Set(data.map(d => d.id));

        const idtoremove = new Set();
        logs.forEach(log => {
            const configId = path.basename(log).split('_')[1];
            if (!idlist.has(configId)) {
                idtoremove.add(configId);
                fs.unlink(log, err => {
                    if (err) {
                        console.error(`Error removing file ${log}: ${err}`);
                    }
                    else {
                        console.log(log, 'removed as not valid configId')
                    }
                });
            }
        });
    });
}


function uploadFile(file) {
    console.log(`Uploading file: ${file}`);

    try {
        // Check if file exists
        if (!fs.existsSync(file)) {
            throw new Error("File not found: " + file);
        }
    } catch (error) {
        console.log(error);

        // Remove any duplicate rows from ziprecord
        const uploadFileList = fs.readFileSync(ziprecord, 'utf8').trim().split('\n');
        const uniqueFileList = [...new Set(uploadFileList)];
        if (uniqueFileList.length < uploadFileList.length) {
            console.log("Found duplicate rows in ziprecord, removing...");
            fs.writeFileSync(ziprecord, uniqueFileList.join('\n') + '\n');
        }

        // Remove row for missing file from ziprecord
        const index = uniqueFileList.indexOf(file);
        if (index > -1) {
            console.log(`Removing missing file from ziprecord: ${file}`);
            uniqueFileList.splice(index, 1);
            fs.writeFileSync(ziprecord, uniqueFileList.join('\n') + '\n');
        }

        return;
    }

    console.log(`Uploading ${file} to ${properties.get('CONFIG_URL_PREFIX')}/attachments/data-points/upload`);

    const options = {
        method: 'POST',
        timeout: timeInterval,
        url: `${properties.get('CONFIG_URL_PREFIX')}/attachments/data-points/upload`,
        headers: {},
        formData: {
            file: {
                value: fs.createReadStream(file),
                options: {
                    filename: file,
                    contentType: null,
                },
            },
        },
    };

    return new Promise((resolve, reject) => {
        request(options, (error, response) => {
            if (error) {
                console.error(`Error uploading ${file}: ${error}`);
                return reject(error);
            }

            if (response.statusCode !== 200) {
                console.error(`Failed to upload ${file}, status code: ${response.statusCode}`);
                return reject(new Error(`Failed to upload ${file}, status code: ${response.statusCode}`));
            }

            console.log(`Uploaded file: ${file}`);

            // Delete the uploaded file
            fs.unlink(file, (error) => {
                if (error) {
                    console.error(`Error deleting file ${file}: ${error}`);
                } else {
                    console.log(`Deleted file: ${file}`);
                }
            });

            // Remove file from uploadFileList and update ziprecord
            const uploadFileList = fs.readFileSync(ziprecord, 'utf8').trim().split('\n');
            const index = uploadFileList.indexOf(file);
            if (index > -1) {
                uploadFileList.splice(index, 1);
                fs.writeFileSync(ziprecord, uploadFileList.join('\n') + '\n');
            }

            resolve(file);
        });
    });
}



function checkFile(file) {
    try {
        // create schedule file once
        if (fs.existsSync(scheduleFile)) {
            return true
        } else {
            return false
        }
    } catch (err) {
        console.error(err)
        return false
    }
}

//local variables
const path_for_logs = __dirname + '\\logs';
const scheduleFile = __dirname + "\\scheduled"

function shuffle(array) {
    let currentIndex = array.length, randomIndex;

    // While there remain elements to shuffle.
    while (currentIndex != 0) {

        // Pick a remaining element.
        randomIndex = Math.floor(Math.random() * currentIndex);
        currentIndex--;

        // And swap it with the current element.
        [array[currentIndex], array[randomIndex]] = [
            array[randomIndex], array[currentIndex]];
    }

    return array;
}

if (!checkFile(scheduleFile)) {
    fs.writeFileSync(scheduleFile, "true")
    const sched_create = exec('create_scheduler.bat', { "cwd": __dirname })
    sched_create.stdout.pipe(process.stdout)
    sched_create.on('exit', function () {
        fs.appendFileSync(__dirname + "/test.txt", "Scheduler Installed\n");
        fs.closeSync(fs.openSync(scheduleFile, 'w'))
    });
}

function todayLastHourLogStr() {
    let now = new Date();
    now.setHours(now.getHours() + 5);
    now.setMinutes(now.getMinutes() + 30);
    let year = now.getFullYear();
    let month = now.getMonth() + 1;
    let day = now.getDate();
    let hour = now.getHours();

    // Pad the month, day, and hour with leading zeros if needed
    month = month.toString().padStart(2, '0');
    day = day.toString().padStart(2, '0');
    hour = hour.toString().padStart(2, '0');

    // Construct the formatted date string with current hour of day
    let formattedDate = `${year}${month}${day}_${hour}`;

    console.log("formattedDate", formattedDate)
    return formattedDate;
}

function todayLogStr() {
    // today string generate
    let today = new Date()
    let yyyy = today.getFullYear();
    let mm = today.getMonth() + 1;
    let dd = today.getDate();

    if (dd < 10) dd = '0' + dd;
    if (mm < 10) mm = '0' + mm;

    let formattedToday = yyyy.toString() + mm.toString() + dd.toString();
    console.log("formattedToday", formattedToday)
    return formattedToday
}


function process1() {
    let files = fs.readdirSync(path_for_logs)
    // let sourceFiles = shuffle(files.filter(file => file.endsWith(".txt")))
    let sourceFiles = shuffle(files.filter(file => file.endsWith(".txt") || file.endsWith(".gz")));

    // console.log("kk path",path_for_logs);
    // console.log("kk source file",sourceFiles)
    sourceFiles = sourceFiles.map(x => __dirname + "\\logs\\" + x)
    // let tdls = todayLastHourLogStr() //this to call only to filter current hour of today YYYYMMDD_HH
    let td = todayLogStr() //this is to call only to filter today YYYYMMDD
    sourceFiles = sourceFiles.filter((logfile) => {
        return !logfile.includes(td);
    })
    console.log("raw files list")
    console.log(sourceFiles)

    if (sourceFiles.length > 0) {
        try {
            compressFile(sourceFiles[0])
        } catch (error) {
            console.error(error)
        }
    }
}

function process2() {
    try {

        // console.log("process 2 ::::::::::;");
        //removeUnwantedLogs()
        let uploadFileList = fs.readFileSync(ziprecord, { encoding: 'utf8', flag: 'r' })
        uploadFileList = uploadFileList.split("\n").filter(x => x != "")
        if (uploadFileList.length === 0) {
            try {
                const gzFiles = fs.readdirSync(path_for_logs).filter(file => file.endsWith('.gz')).map(file => `${path_for_logs}\\${file}`);
                if (gzFiles.length === 0) {
                    console.log('Skipping: no files to left to upload');
                    return; // end the function execution
                } else {
                    fs.writeFileSync(ziprecord, gzFiles.join('\n'), { encoding: 'utf8', flag: 'w' });
                    console.log(`Added ${gzFiles.length} files to ziprecord`);
                    return; // end the function execution
                }
            } catch (error) {
                console.log(`Error reading ${pathForLogs} directory: ${error}`);
                return; // end the function execution
            }
        } else {
            // process upload file procedure
            try {
                const current_zipped_file = uploadFileList[0];
                uploadFile(current_zipped_file);
            } catch (error) {
                console.log(`Failed to upload file: ${error}`);
                console.log(uploadFileList[0]);
            }
        }
    } catch (error) {
        console.log("Skipping: compression should kick first")
    }
}



setInterval(() => {
    process1()
    process2()
    // deleteOldLogs(path_for_logs);

}, timeInterval);

process1()
process2()
// deleteOldLogs(path_for_logs);

