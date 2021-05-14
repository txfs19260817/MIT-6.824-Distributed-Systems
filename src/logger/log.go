package logger

import (
	"io"
	"io/ioutil"

	"github.com/sirupsen/logrus"
)

const LogFilePath = "logs/log.log"

// Logger is a new instance of the logrus logger.
var Logger = logrus.New()

func init() {
	// Logger.SetFormatter(&logrus.JSONFormatter{})
	// Logger.SetLevel(logrus.DebugLevel)
	Logger.SetReportCaller(true)

	//logFile, err := os.OpenFile(LogFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if err == nil {
	//	Logger.Out = io.MultiWriter(os.Stdout, logFile)
	//	return
	//}
	Logger.Out = io.MultiWriter(ioutil.Discard)
}
