package gnidump_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGnidump(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gnidump Suite")
}
