package pimp

import "testing"

func TestParseMysqlshVersion(t *testing.T) {
	for _, tt := range []struct {
		name string
		out  string
		want mysqlshVersion
	}{
		{
			name: "macos",
			out:  "mysqlsh   Ver 8.0.33 for macos13.0 on arm64 - for MySQL 8.0.33 (MySQL Community Server (GPL))\n",
			want: mysqlshVersion{8, 0, 33},
		},
		{
			name: "linux",
			out:  "mysqlsh   Ver 8.0.32 for Linux on x86_64 - for MySQL 8.0.32 (MySQL Community Server (GPL))\n",
			want: mysqlshVersion{8, 0, 32},
		},
		{
			// the trailing MySQL version differs from the shell's here, which
			// is why only the first triple may be read
			name: "shell and server versions differ",
			out:  "mysqlsh   Ver 8.4.0 for Linux on x86_64 - for MySQL 9.1.0 (MySQL Community Server (GPL))\n",
			want: mysqlshVersion{8, 4, 0},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMysqlshVersion(tt.out)
			if err != nil {
				t.Fatalf("parseMysqlshVersion() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("parseMysqlshVersion() = %v, want %v", got, tt.want)
			}
		})
	}

	if _, err := parseMysqlshVersion("command not found"); err == nil {
		t.Error("parseMysqlshVersion() of unparsable output should fail")
	}
}

func TestMysqlshVersionOlderThan(t *testing.T) {
	for _, tt := range []struct {
		got  mysqlshVersion
		want bool
	}{
		{mysqlshVersion{8, 0, 32}, true},
		{mysqlshVersion{8, 0, 33}, false},
		{mysqlshVersion{8, 0, 34}, false},
		{mysqlshVersion{7, 9, 99}, true},
		// a newer major must pass even though its patch is lower
		{mysqlshVersion{9, 0, 1}, false},
		{mysqlshVersion{8, 1, 0}, false},
	} {
		if got := tt.got.olderThan(minMysqlshVersion); got != tt.want {
			t.Errorf("%v.olderThan(%v) = %v, want %v", tt.got, minMysqlshVersion, got, tt.want)
		}
	}
}
