package util

import "fmt"

const debugMode = false

const colorReset = "\033[0m"
const colorRed = "\033[31m"
const colorGreen = "\033[32m"
const colorYellow = "\033[33m"
const colorBlue = "\033[34m"
const colorPurple = "\033[35m"
const colorCyan = "\033[36m"
const colorWhite = "\033[37m"

func PrintfRed(format string, args ...interface{}) {
	if !debugMode {
		return
	}
	fmt.Printf(string(colorRed)+format+string(colorReset), args...)
}
func PrintfGreen(format string, args ...interface{}) {
	if !debugMode {
		return
	}
	fmt.Printf(string(colorGreen)+format+string(colorReset), args...)
}
func PrintfYellow(format string, args ...interface{}) {
	if !debugMode {
		return
	}
	fmt.Printf(string(colorYellow)+format+string(colorReset), args...)
}
func PrintfBlue(format string, args ...interface{}) {
	if !debugMode {
		return
	}
	fmt.Printf(string(colorBlue)+format+string(colorReset), args...)
}
func PrintfPurple(format string, args ...interface{}) {
	if !debugMode {
		return
	}
	fmt.Printf(string(colorPurple)+format+string(colorReset), args...)
}
func PrintfCyan(format string, args ...interface{}) {
	if !debugMode {
		return
	}
	fmt.Printf(string(colorCyan)+format+string(colorReset), args...)
}
func PrintfWhite(format string, args ...interface{}) {
	if !debugMode {
		return
	}
	fmt.Printf(string(colorWhite)+format+string(colorReset), args...)
}
