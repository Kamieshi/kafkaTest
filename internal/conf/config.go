package conf

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
)

type config struct {
	POSTGRES_PASSWORD, POSTGRES_USER, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT string
}

func GetConfig() (*config, error) {
	err := godotenv.Load("../../.env")
	if err != nil {
		return nil, fmt.Errorf("conf.GetConfig: %v", err)
	}
	var c config
	c.POSTGRES_DB = os.Getenv("POSTGRES_DB")
	c.POSTGRES_PASSWORD = os.Getenv("POSTGRES_PASSWORD")
	c.POSTGRES_USER = os.Getenv("POSTGRES_USER")
	c.POSTGRES_HOST = os.Getenv("POSTGRES_HOST")
	c.POSTGRES_PORT = os.Getenv("POSTGRES_PORT")
	return &c, err
}
