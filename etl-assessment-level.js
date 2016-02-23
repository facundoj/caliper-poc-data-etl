'use strict';

var fs      = require('fs'),
    path    = require('path'),
    rimraf  = require('rimraf');

var rowsToExtract, inputPath;

if (process.argv.length < 3) {
    console.log('Usage: node etl-assessment-level <assessments-csv> [rows-to-extract]');
    return -1;
} else {
    inputPath = path.normalize(process.argv[2]);
    rowsToExtract = +process.argv[3] || Infinity;
}

// identity_id,login_identity_id,school_id,assessment_id,assessment_version,date_submitted,assessment_type_id,assessment_type,attempt_id,attemptnumber,is_mastered,score_earned,score_posible
var ASSESSMENT_PATTERN = /(.+),(.+),(.+),(.+),(.+),(.+),(.+),(.+),(.+),(.+),(.+),(.+),(.+)/;

// Cleaning target
rimraf('target', function(err) {
    fs.mkdir('target', function(err) {
        start();
    });
});

function start() {
    // Opening file
    fs.readFile(inputPath, 'utf-8', function (err, content) {
        if (err) {
            console.log('Not a valid path: ', process.argv[2]);
            return;
        }

        // Getting rows from file content
        var rows = content
            .trim()
            .split('\n')
            .slice(1);

        // Iterating through attempts - 1 file per attempt
        rowsToExtract = rowsToExtract > rows.length ? rows.length : rowsToExtract;
        for (let i = 0; i < rowsToExtract; i++) {
            let row = rows[i],
                res = row.trim().match(ASSESSMENT_PATTERN);

            rows[i] = null;

            // If doesn't match the pattern
            if (!res) {
                continue;
            }

            parseAssessmentAttempt(new EventCommand(res));
        }

    });
}

function EventCommand(fields) {
    if (!this instanceof EventCommand) {
        return new EventCommand(fields);
    }

    this.student = fields[1];
    this.assessmentId = fields[4];
    this.assessmentVersion = fields[5];
    this.attemptId = fields[9];
    this.attemptCount = fields[10];
    this.schoolId = fields[3];
    this.scoreObtained = fields[12];
    this.scorePossible = fields[13];
}

EventCommand.prototype.getActor = function () {
    return {
        id: this.student
    };
};

EventCommand.prototype.getAssessment = function () {
    return {
        id: this.assessmentId,
        version: this.assessmentVersion
    };
};

EventCommand.prototype.getAttempt = function () {
    return {
        id: this.attemptId,
        count : +this.attemptCount
    };
};

EventCommand.prototype.getSchool = function () {
    return {
        id: this.schoolId
    };
};

EventCommand.prototype.getScore = function () {
    return {
        totalScore: this.scoreObtained,
        normalScore: this.scorePossible
    };
};

function parseAssessmentAttempt(command) {
    var events = [];
    events.push(parseAssessmentStartEvent(command));
    events.push(parseAssessmentEndEvent(command));
    events.push(parseAssessmentOutcomeEvent(command));

    var filename = path.normalize('./target/events-attempt_' + command.attemptId + '-' + Date.now() + '.json');

    fs.writeFile(filename, JSON.stringify(events), function(err) {
        if (err) {
            console.log('Couldn\'t create ' + filename);
        } else {
            console.log('Created ' + filename);
        }
    });
}

function parseAssessmentStartEvent(command) {
    return {
        type: 'AssessmentEvent',
        values: {
            action: 'STARTED',
        },
        actor: command.getActor(),
        object: command.getAssessment(),
        generated: command.getAttempt()
    };
}

function parseAssessmentEndEvent(command) {
    return {
        type: 'AssessmentEvent',
        values: {
            action: 'SUBMITTED',
        },
        actor: command.getActor(),
        object: command.getAssessment(),
        generated: command.getAttempt()
    };
}

function parseAssessmentOutcomeEvent(command) {
    return {
        type: 'AssessmentOutcomeEvent',
        values: {
            action: 'GRADED',
            actor: command.getActor(),
            organization: command.getSchool(),
            assessment: command.getAssessment(),
            object: command.getAttempt(),
            generated: command.getScore()
        }
    };
}
