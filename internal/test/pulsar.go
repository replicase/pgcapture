package test

func GetPulsarURL() string {
	return Env("PULSAR_URL", "pulsar://127.0.0.1:6650")
}

func GetPulsarAdminURL() string {
	return Env("PULSAR_ADMIN_URL", "http://127.0.0.1:8080")
}
