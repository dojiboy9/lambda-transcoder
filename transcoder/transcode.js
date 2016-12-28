process.env['PATH'] = process.env['PATH'] + ':' + process.env['LAMBDA_TASK_ROOT'];
var aws = require('aws-sdk');
var s3 = new aws.S3({apiVersion: '2006-03-01' });
var sns = new aws.SNS();
var ffmpeg = require('fluent-ffmpeg');
var async = require('async');
var path = require('path');
var fs = require('fs');
// use external s3Stream module to stream to s3 and keep memory footprint low
var s3Stream = require('s3-upload-stream')(new aws.S3());

exports.handler = function(event, context) {
  // write event to console log
  console.log(JSON.stringify(event, null, '  '));

  // get s3 object info from event
  var bucket = event.Records[0].s3.bucket.name;
  var srcKey = event.Records[0].s3.object.key;
  var dstKey = 'transcoded-' + srcKey + ".mp4";

  console.log('Transcoding', srcKey, 'into', dstKey);

  // infer input format
  var typeMatch = srcKey.match(/\.([^.]*)$/);
  if (!typeMatch) {
    console.error('Unable to infer file type for key ' + srcKey);
    context.done(null,'');
    return;
  }

  var fileType = typeMatch[1].toLowerCase();
  if (fileType != "mp4" && fileType != 'mov' && fileType != 'avi' && fileType != 'qt') {
    console.log('Skipping non-video file');
    context.done(null,'');
    return;
  }

  var fileName = path.join('/tmp', path.basename(srcKey));
  var outputFileName = fileName + '.mp4';

  async.waterfall( [
  function download(next) {
    // set source s3 object
    console.log("Getting object from S3: ", srcKey);
    var sourceStream = s3.getObject({ Bucket: bucket, Key: srcKey }).createReadStream();

    // write to file
    console.log("Downloading to file: ", fileName);
    var fileWriteStream = fs.createWriteStream(fileName);

    fileWriteStream
      .on('close', function() {
        console.log('Done downloading the file');
        next();
      })
      .on('error', function(err) {
        console.log("Error writing file");
        console.error(err);
        next(err);
      })

    sourceStream.pipe(fileWriteStream);
  },
  function transform(next) {
    // transcode file
    console.log("Transcoding object");
    var proc = new ffmpeg(fileName);

    // set path to FFmpeg binary
    proc.setFfmpegPath(process.env['LAMBDA_TASK_ROOT'] + "/bin/ffmpeg");

    // set size, format, target stream, options that allow mp4 streaming and event handlers
    proc
      .on('start', function(commandLine) {
        console.log('Query : ' + commandLine);
      })
      .on('error', function(err, stdout, stderr) {
        console.log("Got an error");
        console.log("ffmpeg stdout:\n" + stdout);
        console.log("ffmpeg stderr:\n" + stderr);
        console.error(err);
      })
      .on('end', function() {
        console.log("Done transcoding the file.");
        next();
      })
      .videoCodec('libx264')
      .videoBitrate('320k')
      .audioCodec('copy')
      .output(outputFileName)
      .run();
  },
  function upload(next) {
    // set target s3 object
    var uploadStream = s3Stream.upload({
      Bucket: bucket,
      Key: dstKey
    });
    uploadStream.on('uploaded', function() {
      console.log("Upload complete");
      next();
    });
    uploadStream.on('error', function(error) {
      console.log("Error");
      next(error);
    });

    console.warn('About to start uploading to', dstKey, 'from file', outputFileName);
    var read = fs.createReadStream(outputFileName);
    read.pipe(uploadStream);
  },
  function deleteOriginal(next) {
    // delete original object once transcoding complete
    s3.deleteObject({ Bucket: bucket, Key: srcKey }, next());
  },
  function getVideoUrl(next) {
    // get url to transcoded object
    s3.getSignedUrl('getObject', {Bucket: bucket, Key: dstKey, Expires: 600}, function(err, url) {
      next(null, url);
    });
  },
  function notifyUsers(url, next) {
    return next();
    // TODO (Nomination)
    /*
    var messageParams = {
      Message: 'Video available for download here: ' + url,
      Subject: 'Motion detected from kefa-camera',
      TopicArn: 'arn:aws:sns:eu-west-1:089261358639:kefa-camera'
    };
    sns.publish(messageParams, next);
    */
  }
  ], function (err, data) {
    if (err) {
      console.error(
        'Unable to resize ' + bucket + '/' + srcKey +
        ' and upload to ' + bucket + '/' + dstKey +
        ' due to an error: ' + err
      );
    } else {
      console.log(
        'Successfully resized ' + bucket + '/' + srcKey +
        ' and uploaded to ' + bucket + '/' + dstKey
      );
    }
    context.done();
  }
  );
};
