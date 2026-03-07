package secrets

type Database struct {
	DefaultSecret
	Writer DatabaseMeta `json:"writer"`
	Reader DatabaseMeta `json:"reader"`
}

type DatabaseMeta struct {
	Adapter string `json:"adapter"`
	Params  struct {
		Charset  string `json:"charset"`
		Host     string `json:"host"`
		Port     uint   `json:"port"`
		DBName   string `json:"dbname"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"params"`
}
