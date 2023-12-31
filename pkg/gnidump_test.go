package gnidump_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/gnames/gnidump"
)

var _ = Describe("GNIdump", func() {
	Describe("NewGNIdump", func() {
		It("generates new instance of GNIdump", func() {
			gnd := NewGNIdump()
			Expect(gnd.JobsNum).To(Equal(1))
		})

		It("uses options for setup", func() {
			opts := getOpts()
			gnd := NewGNIdump(opts...)
			Expect(gnd.JobsNum).To(Equal(8))
			Expect(gnd.InputDir).To(Equal("/tmp/gnidump"))
		})
	})
})

func getOpts() []Option {
	var opts []Option
	opts = append(opts, OptInputDir("/tmp/gnidump"))
	opts = append(opts, OptJobsNum(8))
	opts = append(opts, OptMyHost("localhost"))
	opts = append(opts, OptMyUser("root"))
	opts = append(opts, OptMyPass(""))
	opts = append(opts, OptMyDB("gni"))
	opts = append(opts, OptPgHost("localhost"))
	opts = append(opts, OptPgUser("postgres"))
	opts = append(opts, OptPgPass(""))
	opts = append(opts, OptPgDB("gnindex"))
	opts = append(opts, OptPgDB("gnindex"))
	return opts
}
