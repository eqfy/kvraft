package util

import "fmt"

const debugMode = true

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
)

var interfaceSlice = make([]interface{}, 1)

// Returns the number of bits written and any write error occured
// Typically, do not need to worry about the return value
func PrintfRed(format string, args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	return fmt.Printf(string(colorRed)+format+string(colorReset), args...)
}
func PrintfGreen(format string, args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	return fmt.Printf(string(colorGreen)+format+string(colorReset), args...)
}
func PrintfYellow(format string, args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	return fmt.Printf(string(colorYellow)+format+string(colorReset), args...)
}
func PrintfBlue(format string, args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	return fmt.Printf(string(colorBlue)+format+string(colorReset), args...)
}
func PrintfPurple(format string, args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	return fmt.Printf(string(colorPurple)+format+string(colorReset), args...)
}
func PrintfCyan(format string, args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	return fmt.Printf(string(colorCyan)+format+string(colorReset), args...)
}
func PrintfWhite(format string, args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	return fmt.Printf(string(colorWhite)+format+string(colorReset), args...)
}
func PrintlnRed(args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	interfaceSlice[0] = colorRed
	return fmt.Println(append(append(interfaceSlice, args...), colorReset)...)
}
func PrintlnGreen(args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	interfaceSlice[0] = colorGreen
	return fmt.Println(append(append(interfaceSlice, args...), colorReset)...)
}
func PrintlnYellow(args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	interfaceSlice[0] = colorYellow
	return fmt.Println(append(append(interfaceSlice, args...), colorReset)...)
}
func PrintlnBlue(args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	interfaceSlice[0] = colorBlue
	return fmt.Println(append(append(interfaceSlice, args...), colorReset)...)
}
func PrintlnPurple(args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	interfaceSlice[0] = colorPurple
	return fmt.Println(append(append(interfaceSlice, args...), colorReset)...)
}
func PrintlnCyan(args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	interfaceSlice[0] = colorCyan
	return fmt.Println(append(append(interfaceSlice, args...), colorReset)...)
}
func PrintlnWhite(args ...interface{}) (n int, err error) {
	if !debugMode {
		return
	}
	interfaceSlice[0] = colorWhite
	return fmt.Println(append(append(interfaceSlice, args...), colorReset)...)
}
