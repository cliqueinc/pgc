package main

import "testing"

func TestInitPGC(t *testing.T) {
	initPGC("pgc_local", "localhost", "", "", "disable", "")
}

func TestInitPGCPanicsPort(t *testing.T) {
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("TestInitPGCPanicsPort should have panicked")
			}
		}()
		initPGC("pgc_local", "localhost", "", "abc", "disable", "")
	}()
}

func TestInitPGCPanicsConnect(t *testing.T) {
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("TestInitPGCPanicsConnect should have panicked")
			}
		}()
		initPGC("pgc_local", "localho", "", "", "disable", "")
	}()
}
