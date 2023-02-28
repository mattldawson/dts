package core

type struct Endpoint {
  // Globus endpoint URLs (if any)
  Globus map[string]string `yaml:"globus"`
}
