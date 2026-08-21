package releaseversion

import "testing"

func TestParseTag(t *testing.T) {
	t.Parallel()
	valid := []string{"v0.0.0", "v1.2.3", "v1.2.3-rc.0", "v18446744073709551615.0.1"}
	for _, tag := range valid {
		tag := tag
		t.Run(tag, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseTag(tag); err != nil {
				t.Fatalf("ParseTag(%q): %v", tag, err)
			}
		})
	}
	invalid := []string{
		"", "1.2.3", "v1.2", "v1.2.3.4", "v01.2.3", "v1.02.3", "v1.2.03",
		"v1.2.3-alpha.1", "v1.2.3-rc", "v1.2.3-rc.01", "v1.2.3+build", "v1.2.3-rc.1+build",
		"v18446744073709551616.0.0",
	}
	for _, tag := range invalid {
		tag := tag
		t.Run("invalid_"+tag, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseTag(tag); err == nil {
				t.Fatalf("ParseTag(%q) succeeded", tag)
			}
		})
	}
}

func TestCompare(t *testing.T) {
	t.Parallel()
	ordered := []string{"v1.2.2", "v1.2.3-rc.0", "v1.2.3-rc.1", "v1.2.3", "v1.3.0", "v2.0.0"}
	for index := 0; index < len(ordered)-1; index++ {
		left, _ := ParseTag(ordered[index])
		right, _ := ParseTag(ordered[index+1])
		if left.Compare(right) >= 0 {
			t.Fatalf("%s does not sort before %s", ordered[index], ordered[index+1])
		}
		if right.Compare(left) <= 0 {
			t.Fatalf("%s does not sort after %s", ordered[index+1], ordered[index])
		}
	}
}
