var exec = require('child_process').exec,
    mqtt = require('mqtt'),
    readline = require('readline'),
    fs = require('fs'),
    https = require('https'),
    chokidar = require('chokidar'),
    md5lib = require('js-md5'),
    sqlite3 = require('sqlite3');

var _context = null;

var _configLocation = "installer/kiiconfig.json";

var _version = "0.0.1a";
var _currentState = {};

var _db = null;

var _intervals = {};

var kii = require('kii-cloud-sdk').create();

function randomString(len) {
    var text = "";
    var possible = "0123456789ABCDEF";

    for (var i = 0; i < len; i++)
        text += possible.charAt(Math.floor(Math.random() * possible.length));

    return text;
}

function randomUUID() {
    return randomString(8) + "-" + randomString(4) + "-" + randomString(4) + "-" + randomString(4) + "-" + randomString(12);
}

function checkInternet(cb) {
    require('dns').lookup('google.com', function(err) {
        if (err && err.code == "ENOTFOUND") {
            cb(false);
        } else {
            cb(true);
        }
    })
}

function clearIntervals() {
    for (var i in _intervals) {
        _log("Clearing interval: " + i);
        clearInterval(_intervals[i]);
    }
}

var _requestQueue = [];
var _makingRequest = false;

function makeRequest(request) {

    _makingRequest = true;

    _log("Making request: " + JSON.stringify(request));

    kii.Kii.serverCodeEntry(request.endpoint).execute(request.args).then(
        function(params) {
            var entry = params[0];
            var argument = params[1];
            var execResult = params[2];
            var returnedValue = execResult.getReturnedValue();
            var response = returnedValue["returnedValue"];

            _log("Received response: " + JSON.stringify(response));

            if (response.status == "Success" || response.response == "Success" || (!response.status && !response.response)) {
                request.success(response);
            } else {
                request.failure(response);
            }

            _makingRequest = false;
            popRequest();
        },

        function(error) {

            _log("Request error... checking internet...");

            // see if it was an internet error
            checkInternet(function(working) {

                _makingRequest = false;

                // if the internet's working, something else happened, so pop this
                if (working == true) {
                    _log("Internet working, popping request");

                    // only make the failure callback on final failure
                    request.failure(error);
                    popRequest();
                }

                // otherwise, it was a network error, so just fail silently - the interval will try it again later
                else {
                    _log("ERROR: Internet is down");
                }
            })

        }
    );
}

function popRequest() {

    // remove the top element
    _requestQueue.shift();

    // restart from the top
    flushRequests();
}

function flushRequests() {
    if (_requestQueue.length > 0 && !_makingRequest) {
        makeRequest(_requestQueue[0]);
    }
}

function queueRequest(endpoint, args, success, failure) {

    // _requestQueue.push({
    //     'endpoint': endpoint,
    //     'args': args,
    //     'success': success,
    //     'failure': failure
    // });
    makeRequest({
        'endpoint': endpoint,
        'args': args,
        'success': success,
        'failure': failure
    });
    // flushRequests();
}

function initializeDatabase() {

    _log("Initializing database");
    _log(_config.rootDir + 'kii.sqlite');

    _db = new sqlite3.Database(_config.rootDir + 'kii.sqlite');

    _db.serialize(function() {
        var query = "CREATE TABLE IF NOT EXISTS logs ("
        query += "md5 TEXT, "
        query += "log TEXT, "
        query += "status INTEGER NOT NULL DEFAULT 0, " // -1 = failed | 0 = unsent | 1 = pending | 2 = success
        query += "CONSTRAINT md5_unique UNIQUE(md5)"
        query += ")";

        _db.run(query, function(e) {
            if (e != null) {
                _log("Unexpected error creating table: " + e);
            }
        });

        // reset any pending to failed on launch
        _db.prepare("UPDATE logs SET status=-1 WHERE status=1").run(function(e) {
            if (e != null) {
                _log("Unexpected error updating log status on launch: " + e);
            }
        });
    });
}

function addLogRecord(log) {

    // verify that it's valid json, otherwise record log if needed and bail
    try {
        JSON.parse(log);
    } catch (e) {
        if (log != "") {
            _log("Invalid JSON saved: " + log);
        }
        return;
    }

    // calculate the md5
    var md5 = md5lib(log);

    _db.prepare("INSERT INTO logs (md5, log, status) VALUES (?, ?, 0)").run(md5, log, function(e) {
        // on duplicates, we'll expect e == SQLITE_CONSTRAINT: UNIQUE constraint failed: logs.md5
        if (e != null) {
            if (e.code != "SQLITE_CONSTRAINT") {
                _log("Unexpected error inserting log: " + e);
            }
        } else {
            _log("Record added: " + md5);
        }
    });

    return md5;
}

function updateLogStatus(md5, status) {

    _log("Updating record: " + md5 + " => " + status);

    _db.prepare("UPDATE logs SET status=? WHERE md5=?").run(status, md5, function(e) {
        // on duplicates, we'll expect e == SQLITE_CONSTRAINT: UNIQUE constraint failed: logs.md5
        if (e != null) {
            _log("Unexpected error updating log: " + e);
        }
    });
}

function saveLog(md5, log) {
    _log("Saving log " + md5 + " => " + log);

    // set to 'pending' so we don't double-send
    updateLogStatus(md5, 1);

    var jsonLog = JSON.parse(log);

    var arg = {
        "vendorThingId": _config.kiiVendorThingID,
        "thingPassword": _config.kiiThingPassword,
        "entities": [{ "transactionId": md5, "data": jsonLog }],
    };

    queueRequest("uploadLogEntities", arg, function(response) {
        if (response.entities) {
            for (var i = 0; i < response.entities.length; i++) {
                var e = response.entities[i];

                _log("Entity: " + JSON.stringify(e));

                if (e.status == "Fail") {
                    _log("Error saving log: failed entity. Storing as failed status, no-retry. " + JSON.stringify(e));
                } else {
                    _log("Saving new log: " + e.transactionId);
                }

                // set to 3 if we got a specific kii error, otherwise we're good, so set to 2
                updateLogStatus(e.transactionId, e.status == "Fail" ? 3 : 2);
            }
        } else {
            _log("Error saving log - invalid response (no entities)");
        }

    }, function(error) {
        _log("Error saving log: " + JSON.stringify(error));

        // this errors out in a way that it'll be retried
        updateLogStatus(md5, -1);
    });

}


function sendLogRecords() {
    _db.each("SELECT * FROM logs WHERE status < 1", function(err, row) {
        saveLog(row.md5, row.log);
    });
}

function installUpdate(updateUrl) {
    _log("Installing update from: " + updateUrl);

    var dir = _config.rootDir + "tmp\\";
    var dest = dir + "update.exe";

    // make sure the temp dir exists
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }

    var cb = function (err) {
        if (err == null) {
            _log("Executing installer");
            exec(dest);
        } else {
            _log("Error: Unable to start installer");
        }
    }

    var file = fs.createWriteStream(dest);
    var request = https.get(updateUrl, function (response) {
        response.pipe(file);
        file.on('finish', function () {
            file.close(cb);  // close() is async, call cb after close completes.
        });
    }).on('error', function (err) { // Handle errors
        fs.unlink(dest); // Delete the file async. (But we don't check the result)
        if (cb) cb(err.message);
    });
}

function checkForUpdates() {
    _log("Checking for updates");

    var obj = _context.bucketWithName('chris_do_not_delete').createObjectWithID('versionbundle');
    obj.refresh({
        success: function (theObject) {
            if(theObject.get("version") != _config['version']) {
                _log("Install new version " + _config['version'] + " => " + theObject.get("version"));

                // kill the interval so we don't restart the process
                // TODO: clear all intervals(?)
                clearInterval(_intervals['version']);
                _intervals['version'] = null;

                _log("Publishing file...");

                // publish the new file
                var expiresIn = 60 * 5; // Expires in 5 minutes.
                theObject.publishBodyExpiresIn(expiresIn, {
                    success: function (theObject, publishedUrl) {

                        // start the install
                        installUpdate(publishedUrl);

                    },
                    failure: function (theObject, errorString) {
                        // Error handling
                        _log("Error publishing file: " + errorString);
                    }
                });

            }
        },
        failure: function (theObject, errorString) {
            _log("Unable to get object: " + errorString);
        }
    });
}


function parseFileLogs(path) {

    var lineReader = readline.createInterface({
        input: fs.createReadStream(path)
    });

    lineReader.on('line', function(line) {
        addLogRecord(line.trim());
    });

    // make log requests after a second so we have time to save to db.
    // if it doesn't send here, it'll get cleaned up by the interval every 60s
    setTimeout(sendLogRecords, 1000);
}


function listenToLogs() {

    var watcher = chokidar.watch('file', {
        ignored: /(^|[\/\\])\../,
        persistent: true,
        ignoreInitial: true,
    });

    watcher
        .on('add', function(path) {
            _log("File has been added: " + path);
            parseFileLogs(path);
        })
        .on('change', function(path) {
            _log("File has been changed: " + path);
            parseFileLogs(path);
        });

    watcher.add(_config.logPath);
}

function listenForAgentConfigChanges() {

    var watcher = chokidar.watch('file', {
        ignored: /(^|[\/\\])\../,
        persistent: true,
        ignoreInitial: true,
    });

    watcher
        .on('change', function(path) {
            _log("File has been changed: " + path);
            loadAgentConfig();
        });

    watcher.add(_configLocation);
}

function saveAgentConfig() {
    fs.writeFile(_configLocation, JSON.stringify(_config, null, "\t"), function(err) {
        if (err) {
            _log("Error writing device identifier to config file: " + err);
        }

        _log("Config file saved: " + JSON.stringify(_config));
    });
}

function setAgentConfig(key, value) {
    _log("Saving config " + key + " => " + value);
    _config[key] = value;
    saveAgentConfig();
}

function loadAgentConfig() {

    var fileContents = null;

    try {
        fileContents = fs.readFileSync(_configLocation, 'utf8');
    }

    // this is for testing purposes -- the file should exist from install
    catch (e) {
        _log("Unable to open config location: " + _configLocation);
        _log(e);
        _configLocation = "test_temp/kiiconfig.json";
        fileContents = fs.readFileSync(_configLocation, 'utf8');
    }

    _config = JSON.parse(fileContents.trim());

    _config.version = _version;

    _log("Loaded config:");
    _log("==============");

    for (var key in _config) {
        _log(key + " => " + JSON.stringify(_config[key]));
    }

    _log("==============");

    // check to see if the device identifier key is there
    if (!_config['uniqueDeviceIdentifier']) {
        _log("No device identifier");

        // TODO: grab serial number/imei if available

        // generate a random string and save it
        setAgentConfig('uniqueDeviceIdentifier', randomUUID());
    }

    // we want this to reload each time the config changes
    kii.Kii.initializeWithSite(_config.kiiAppID, _config.kiiAppKey, _config.kiiAppURL);

    // this will call it on file change, and on first launch
    uploadAgentConfig();


    // clear the intervals
    clearIntervals();

    // (re-)initialize based on config values
    _log("Initializing intervals");
    // _intervals['version'] = setInterval(checkForUpdates, _config['updateIntervalMillis']['version']);
    _intervals['upload'] = setInterval(sendLogRecords, _config['updateIntervalMillis']['upload']);
    _intervals['agentConfig'] = setInterval(downloadAgentConfig, _config['updateIntervalMillis']['agentConfig']);
    _intervals['ping'] = setInterval(sendPing, _config['updateIntervalMillis']['ping']);
    _intervals['request'] = setInterval(flushRequests, 60*1000);
}

function downloadAgentConfig() {

    var arg = {
        "vendorThingId": _config.kiiVendorThingID,
        "thingPassword": _config.kiiThingPassword,
    };

    queueRequest("getAgentConfig", arg, function(response) {
        if (response['data']) {
            _config = response['data'];
            saveAgentConfig();
        }
    }, function(error) {
        _log("Error downloading agent config: " + JSON.stringify(error));
    });

}

function uploadAgentConfig() {

    var arg = {
        "vendorThingId": _config.kiiVendorThingID,
        "thingPassword": _config.kiiThingPassword,
        "data": _config
    };

    queueRequest("updateAgentConfig", arg, function(response) {
        // this is just an upload... so... don't care...
    }, function(error) {
        _log("Error uploading agent config: " + JSON.stringify(error));
    });

}

function listenForMachineConfigChanges() {

    var watcher = chokidar.watch('file', {
        ignored: /(^|[\/\\])\../,
        persistent: true,
        ignoreInitial: true,
    });

    watcher.on('change', function(path) {
        _log("File has been changed: " + path);
        uploadMachineConfig();
    });

    watcher.add(_config['vendingConfigPath']);
}

// save the new json to the file, overwriting (eek) anything in the product config(?)
function updateMachineConfig(newJSON) {
    fs.writeFile(_config['vendingConfigPath'], JSON.stringify(newJSON, null, "\t"), function(err) {
        if (err) {
            _log("Error writing to machine config file: " + err);
        }

        _log("Machine config file saved: " + JSON.stringify(_config));
    });
}

function uploadMachineConfig() {

    // TODO: we want to upload this on launch and on change (above).
    // it should be a semaphore and disallow any other config requests until complete
    // if there was a failure, we need to cache and try again later.
    // we should block any downstream updates until this is resolved.
    // machine should have dominance

    _log("Uploading machine config");

    // read in teh file
    fs.readFile(_config['vendingConfigPath'], 'utf8', function(err, data) {
        if (err) {
            _log("Unable to read machine config file to upload: " + err);
        } else {

            try {
                var arg = {
                    "vendorThingId": _config.kiiVendorThingID,
                    "thingPassword": _config.kiiThingPassword,

                    // TODO: any translations for RFD need to be here
                    "data": JSON.parse(data)
                };
                queueRequest("updateVendingConfig", arg, function(response) {
                    // this is just an upload... so... don't care...
                }, function(error) {
                    _log("Error uploading machine config: " + JSON.stringify(error));
                });

            } catch (e) {
                _log("Unable to upload machine config: " + e);
            }


        }
    });
}

function _log(message) {
    console.log((new Date()) + " | " + message + "\r\n");
}

function sendPing() {

    var arg = {
        "vendorThingId": _config.kiiVendorThingID,
        "thingPassword": _config.kiiThingPassword,
        "data": {
            "lastPingTimeMillis": _config.lastPingTimeMillis,
        }
    };

    queueRequest("ping", arg, function(response) {

        // this is a big issue if RFD updates, and the upload doesn't trigger right away
        // so you need to smooth out syncing issues (TODO)
        if (_config['updateMachineConfig'] == true && Object.keys(response['data']).length > 0) {
            updateMachineConfig(response['data']);
        }

        // update our config with the latest
        if (response['data']['currentTimeDTMillis']) {
            setAgentConfig('lastPingTimeMillis', response['data']['currentTimeDTMillis']);
        }
    }, function(error) {
        _log("Error executing ping: " + JSON.stringify(error));
    });
}

function initializeAgent() {

    uploadMachineConfig();
    sendPing();

    _log("Starting listeners");
    listenForAgentConfigChanges();
    listenToLogs();
    listenForMachineConfigChanges();
}

function authenticate() {
    _log("Loading configuration");

    loadAgentConfig();
    initializeDatabase();

    _log("Authenticating machine");

    kii.Kii.authenticateAsThing(_config.kiiVendorThingID, _config.kiiThingPassword).then(
        function(thingAuthContext) {
            _context = thingAuthContext;
            _log("Machine authenticated");
            initializeAgent();
            _log("Kii agent loaded");
        },
        function(error) {
            _log("Error authenticating machine: " + JSON.stringify(error));
            _log("We'll try again in 1 minute...");
            setTimeout(authenticate, 60*1000)
        }
    );
}

authenticate();
