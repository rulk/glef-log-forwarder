package main


import (
	"net"
	"fmt"
	"os"
	"flag"
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"log"
	"encoding/json"
	"gopkg.in/fatih/pool.v2"
)

func main() {

	portPtr := flag.String("port", "", "a port number")
	tokenPtr := flag.String("token", "", "a logz.io token")
	flag.Parse()

	port := *portPtr
	token := *tokenPtr

	if port == ""{
		port = os.Getenv("LF_PORT")
	}
	if token == ""{
		token = os.Getenv("LF_TOKEN")
	}


	rootPEM := `-----BEGIN CERTIFICATE-----
MIIENjCCAx6gAwIBAgIBATANBgkqhkiG9w0BAQUFADBvMQswCQYDVQQGEwJTRTEU
MBIGA1UEChMLQWRkVHJ1c3QgQUIxJjAkBgNVBAsTHUFkZFRydXN0IEV4dGVybmFs
IFRUUCBOZXR3b3JrMSIwIAYDVQQDExlBZGRUcnVzdCBFeHRlcm5hbCBDQSBSb290
MB4XDTAwMDUzMDEwNDgzOFoXDTIwMDUzMDEwNDgzOFowbzELMAkGA1UEBhMCU0Ux
FDASBgNVBAoTC0FkZFRydXN0IEFCMSYwJAYDVQQLEx1BZGRUcnVzdCBFeHRlcm5h
bCBUVFAgTmV0d29yazEiMCAGA1UEAxMZQWRkVHJ1c3QgRXh0ZXJuYWwgQ0EgUm9v
dDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALf3GjPm8gAELTngTlvt
H7xsD821+iO2zt6bETOXpClMfZOfvUq8k+0DGuOPz+VtUFrWlymUWoCwSXrbLpX9
uMq/NzgtHj6RQa1wVsfwTz/oMp50ysiQVOnGXw94nZpAPA6sYapeFI+eh6FqUNzX
mk6vBbOmcZSccbNQYArHE504B4YCqOmoaSYYkKtMsE8jqzpPhNjfzp/haW+710LX
a0Tkx63ubUFfclpxCDezeWWkWaCUN/cALw3CknLa0Dhy2xSoRcRdKn23tNbE7qzN
E0S3ySvdQwAl+mG5aWpYIxG3pzOPVnVZ9c0p10a3CitlttNCbxWyuHv77+ldU9U0
WicCAwEAAaOB3DCB2TAdBgNVHQ4EFgQUrb2YejS0Jvf6xCZU7wO94CTLVBowCwYD
VR0PBAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wgZkGA1UdIwSBkTCBjoAUrb2YejS0
Jvf6xCZU7wO94CTLVBqhc6RxMG8xCzAJBgNVBAYTAlNFMRQwEgYDVQQKEwtBZGRU
cnVzdCBBQjEmMCQGA1UECxMdQWRkVHJ1c3QgRXh0ZXJuYWwgVFRQIE5ldHdvcmsx
IjAgBgNVBAMTGUFkZFRydXN0IEV4dGVybmFsIENBIFJvb3SCAQEwDQYJKoZIhvcN
AQEFBQADggEBALCb4IUlwtYj4g+WBpKdQZic2YR5gdkeWxQHIzZlj7DYd7usQWxH
YINRsPkyPef89iYTx4AWpb9a/IfPeHmJIZriTAcKhjW88t5RxNKWt9x+Tu5w/Rw5
6wwCURQtjr0W4MHfRnXnJK3s9EK0hZNwEGe6nQY1ShjTK3rMUUKhemPR5ruhxSvC
Nr4TDea9Y355e6cJDUCrat2PisP29owaQgVR1EX1n6diIWgVIEM8med8vSTYqZEX
c4g/VhsxOBi0cQ+azcgOno4uG+GMmIPLHzHxREzGBHNJdmAPx/i9F4BrLunMTA5a
mnkPIAou1Z5jJh5VkpTYghdae9C8x49OhgQ=
-----END CERTIFICATE-----`

	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM([]byte(rootPEM))
	if !ok {
		panic("failed to parse root certificate")
	}

	logzio, err := pool.NewChannelPool(1,10, func() (net.Conn, error) {
		log.Println("Making New Connection")
		return tls.Dial("tcp", "listener.logz.io:5052", &tls.Config{
			RootCAs: roots,
		})
	})

	if err != nil{
		panic(err)
	}

	// Listen for incoming connections.
	l, err := net.Listen("tcp", ":"+(port))
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on "  + ":" + (port))
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn, logzio, token)
	}



}

// Handles incoming requests.
func handleRequest(conn net.Conn, p pool.Pool, token string) {

	defer func(){
		log.Println("Connection closed")
		conn.Close()
	}()
	log.Println("Starting new connection")
	reader := bufio.NewReaderSize(conn, 1024)
	tokenMsg := json.RawMessage("\""+token+"\"")
	sourceMsg := json.RawMessage("\"server\"")
	for {
		//data := make([]byte,1024);
		// Read the incoming connection into the buffer.
		arrStr, err := reader.ReadBytes(0)
		//log.Println(string(arrStr))
		if err != nil{
			log.Println(err)
			return
		}

		var objmap map[string]*json.RawMessage
		err = json.Unmarshal(arrStr[:len(arrStr)-1], &objmap)
		if err != nil{
			log.Println(err)
			continue
		}

		objmap["token"] = &tokenMsg
		objmap["source"] = &sourceMsg
		
		mesageStr, msgOk := objmap["message"]
		shMessage, shMsgOk := objmap["short_message"]
		
		if  !msgOk && shMsgOk{
			objmap["message"] = shMessage
			delete(objmap, "short_message")
		}
		
		res, e:= json.Marshal(&objmap)
		if e != nil{
			log.Println(e)
			continue
		}
		//log.Println(string(res))
		for i:=0; i < 3 ; i++{
			conn, err := p.Get()
			if err != nil{ continue }
			if pc, ok := conn.(*pool.PoolConn); ok {
				_,err := conn.Write(res)
				if err != nil{
					log.Println(err)
					pc.MarkUnusable()
					pc.Close()
					continue
				}
				_,err = conn.Write([]byte("\n"))
				if err != nil{
					log.Println(err)
					pc.MarkUnusable()
					pc.Close()
					continue
				}

			}
			conn.Close()
			break
		}


	}
}
