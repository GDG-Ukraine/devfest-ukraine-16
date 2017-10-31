'use strict'

const pubsub = require('@google-cloud/pubsub');
const functions = require('firebase-functions');
const admin = require('firebase-admin');
const rp = require('request-promise');
const retry = require('promise-retry');
const fse = require('fs-extra');
const gcs = require('@google-cloud/storage')();

admin.initializeApp(functions.config().firebase);

exports.saveUserData = require('./users');

const scheduleGenerator = require('./schedule-generator-helper.js').generateSchedule;

exports.saveBackup = functions.database.ref('/{node}').onWrite(event => {

    const maxBackupsCount = 3;

    // We need to upload backups only once, with not generated schedule changes again
    if(event.params.node == 'generated') {
        return;
    }

    const data = event.data.val();

    // With Promises
    fse.writeJson('/tmp/db-data.json', data)
        .then(() => {
            console.log('Successfuly written data to /tmp folder')
        })
        .catch(err => {
            console.error(err)
    });

    // Reference an existing bucket.
    const bucket = gcs.bucket('db-history-to-slack.appspot.com');    
    
    //Check files count. If get to limit - delete thr oldes one
    bucket.getFiles({ prefix: 'backup/db-data', delimiter: '/', autoPaginate: false })
    .then((result) => {
        let files = result[0];        

        console.log(`There are ${files.length} in /backup folder`);

        if (files.length < maxBackupsCount) {
            return;
        }
        else {
            // sort by date desc - oldest first
            const sortedFiles = files.sort(function(a,b) {
                return new Date(a.metadata.updated) - new Date(b.metadata.updated);
            });

            const fileName = sortedFiles[0].name;

            bucket
            .file(fileName)
            .delete()
            .then(() => {
                console.log(`${fileName} deleted.`);
            })
            .catch(err => {
            console.error('ERROR:', err);
            });
        }

       
    })
    .catch(err => console.error(err));

    //Upload a local file to a new file to be created in bucket.
    bucket.upload('/tmp/db-data.json', { destination: `/backup/db-data-${new Date().getTime()}.json` })
        .then(() => {
            console.log('Backup was successfuly uploaded!');
        })
        .catch((err) => {
            console.error(`Error occured during upload: ${err}`);
        });

});

exports.scheduleWrite = functions.database
    .ref("/schedule").onWrite(event => {

        // Notify about DB changes
        if (event.auth.admin) {
            const topic = createTopic('db-changed');

            const data = {
                changedData: event.data._delta,
                dataPath: '/schedule',                
                database: functions.config().firebase.databaseURL
            };            

            // postToSlack(data).then(() => {
            //     console.log("Message sent to Slack");
            //   }).catch(error => {
            //     console.error(error);
            // });

            publishMessage(topic, data);
        }

        const schedulePromise = event.data;
        const sessionsPromise = admin.database().ref('/sessions').once('value');
        const speakersPromise = admin.database().ref('/speakers').once('value');

        return generateScheduleOnChange(schedulePromise, sessionsPromise, speakersPromise);
    });


exports.sessionsWrite = functions.database
    .ref("/sessions").onWrite(event => {

        // Notify about DB changes
        if (event.auth.admin) {
            const topic = createTopic('db-changed');

            const data = {
                changedData: event.data._delta,
                dataPath: '/sessions',                
                database: functions.config().firebase.databaseURL
            };

            // postToSlack(data).then(() => {
            //     console.log("Message sent to Slack");
            //   }).catch(error => {
            //     console.error(error);
            // });

            publishMessage(topic, data);
        }

        const sessionsPromise = event.data;
        const schedulePromise = admin.database().ref('/schedule').once('value');
        const speakersPromise = admin.database().ref('/speakers').once('value');

        return generateScheduleOnChange(schedulePromise, sessionsPromise, speakersPromise);
    });

exports.speakersWrite = functions.database
    .ref("/speakers").onWrite(event => {

        // Notify about DB changes
        if (event.auth.admin) {
            const topic = createTopic('db-changed');

            const data = {
                changedData: event.data._delta,
                dataPath: '/speakers',                
                database: functions.config().firebase.databaseURL
            };

            // postToSlack(data).then(() => {
            //     console.log("Message sent to Slack");
            //   }).catch(error => {
            //     console.error(error);
            // });

            publishMessage(topic, data);
        }

        const speakersPromise = event.data;
        const sessionsPromise = admin.database().ref('/sessions').once('value');
        const schedulePromise = admin.database().ref('/schedule').once('value');

        return generateScheduleOnChange(schedulePromise, sessionsPromise, speakersPromise);
    });

function generateScheduleOnChange(schedulePromise, sessionsPromise, speakersPromise) {
    return Promise.all([schedulePromise, sessionsPromise, speakersPromise])
        .then(([scheduleSnapshot, sessionsSnapshot, speakersSnapshot]) => {

            const scheduleDB = scheduleSnapshot.val();
            const sessionsDB = sessionsSnapshot.val();
            const speakersDB = speakersSnapshot.val();

            const {
                schedule,
                sessions,
                speakers
            } = scheduleGenerator(scheduleDB, sessionsDB, speakersDB);

            admin.database().ref('/generated/schedule').set(schedule);
            admin.database().ref('/generated/sessions').set(sessions);
            admin.database().ref('/generated/speakers').set(speakers);

        })
        .catch(e => console.log('Error at schedule genaration', e));
}

function createTopic(topicName) {
    const pubsubClient = pubsub();

    const topic = pubsubClient.topic(topicName);

    if (topic) {
        console.log(`Topic ${topic.name} already exists.`);
        return topic;
    }

    pubsubClient.createTopic(topicName)
        .then((results) => {
        const topic = results[0];

        console.log(`Topic ${topic.name} created.`);

        return topic;
    });
};

function publishMessage (topic, data) {  

    // const options = {
    //     batching: {
    //         maxMilliseconds: 5000
    //     }
    // }
    // // Create a publisher for the topic (which can include additional batching configuration)
    // const publisher = topic.publisher(options);

    // console.log(`Start publish for ${data.dataPath}`);
  
    // // Publishes the message as a string, e.g. "Hello, world!" or JSON.stringify(someObject)
    // const dataBuffer = Buffer.from(JSON.stringify(data));

    // retry({retries: 5, maxTimeout: 8000}, (retry, number) => {
    //     return publisher.publish(dataBuffer)
    //     .then((results) => {
    //         const messageId = results[0];

    //         console.log(`Message ${messageId} published.`);
    //     })
    //     .catch((err) => {
    //         console.error(`An error occured during publish: ${err}`);
    //         retry(err);
    //     });
    //   }).then((val) => {}, (err) => {
    //     console.log(`An error occured during publish: ${err}`)
    //     reject(err)
    //   });    
  }


  function postToSlack(data) {
    return rp({
      method: 'POST',
      // firebase_db_changes channel hook
      uri: 'https://hooks.slack.com/services/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',
      body: {
          text: `:fire: New changes arrived in ${data.dataPath} :fire:`,
          attachments: [
              {
                  title: `Check ${data.database}`,
                  title_link: `${data.database}${data.dataPath}`,
                  text: `${JSON.stringify(data.changedData)}`,
                  color: "#FFC107"
              }
          ]
      },
      json: true
    });
  }