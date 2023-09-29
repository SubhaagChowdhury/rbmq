package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
rabbitmq variables structure
*/
type RabbitMQ struct {
	Host     string
	Port     int
	Exchange string
	VHost    string
	Durable  bool
	UserName string
	Password string

	InterruptFlag bool
	RetryCount    int
	RetryDelay    int
	SSLFlag       bool
	ConsumerFlag  bool

	PrefetchCount int
	Timeout       int
	Consumer      string
	SSLDetails    map[string]interface{}
	//lastPublishTime       time.Time

	ConsumerQueue amqp.Queue

	Connection *amqp.Connection
	Channel    *amqp.Channel

	NotifyConfirm         chan amqp.Confirmation
	NotifyChannelClose    chan *amqp.Error
	NotifyConnectionClose chan *amqp.Error
	NotifyBlocked         chan amqp.Blocking
}

/*
rabbitmq interface delcarations
*/
type RabbitMQInterface interface {
	Initialize() error
	Connect() error
	connectWithoutSSL() error
	connectWithSSL() error
	Disconnect() error
	Reconnect() error
	Publisher(string, string) error
	queueDeclare(string) (amqp.Queue, error)
	exchangeDeclare() bool
}

func (rmq *RabbitMQ) Initialize() error {
	return nil
}

/*
connection function asdasd
*/
func (rmq *RabbitMQ) Connect() error {
	var err error

	//create connection
	if !rmq.SSLFlag {
		err = rmq.connectWithoutSSL()
	} else {
		err = rmq.connectWithSSL()
	}
	if err != nil {
		return err
	}

	if err == nil {
		rmq.Channel, err = rmq.Connection.Channel()

		if err == nil {
			//check wether client is active or not
			if rmq.Connection.IsClosed() {
				//connection is not active
				return errors.New("connection failed")
			}
		} else {
			//channel couldnt be created
			return errors.New("channel creation failed")
		}

		err = rmq.Channel.Confirm(false)
		if err != nil {
			return fmt.Errorf("channel confirm failed, err :%v", err)
		}

		if err = rmq.exchangeDeclare(); err != nil {
			return err
		}

		if rmq.ConsumerFlag {
			if err = rmq.consumerSetup(); err != nil {
				return err
			}
		}
		rmq.NotifyConfirm = make(chan amqp.Confirmation, 1)
		rmq.Channel.NotifyPublish(rmq.NotifyConfirm)
	}

	return nil
}

func (rmq *RabbitMQ) consumerSetup() error {
	var err error

	rmq.ConsumerQueue, err = rmq.queueDeclare(rmq.Consumer)
	if err != nil {
		return err
	}
	args := make(amqp.Table)
	args["x-max-priority"] = ""
	//bind queue to Exchange
	err = rmq.Channel.QueueBind(
		rmq.ConsumerQueue.Name, //queue name
		rmq.ConsumerQueue.Name, //routing key
		rmq.Exchange,           //Exchange name
		false,                  //no wait
		args,                   //arguments
	)

	if err != nil {
		return err
	}

	err = rmq.Channel.Qos(rmq.PrefetchCount, 0, false)

	if err != nil {
		return err

	}

	rmq.NotifyChannelClose = make(chan *amqp.Error)
	rmq.Channel.NotifyClose(rmq.NotifyChannelClose)
	rmq.NotifyBlocked = make(chan amqp.Blocking)
	rmq.Connection.NotifyBlocked(rmq.NotifyBlocked)
	return nil
}

func (rmq *RabbitMQ) connectWithoutSSL() error {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", rmq.UserName, rmq.Password, rmq.Host, rmq.Port, rmq.VHost))
	if err != nil {
		//connection failed
		return err
	}

	//store connection and channel
	rmq.Connection = conn
	return nil
}

func (rmq *RabbitMQ) connectWithSSL() error {

	cfg := new(tls.Config)
	cfg.RootCAs = x509.NewCertPool()

	file, err := os.Open(rmq.SSLDetails["ca-cert"].(string))
	if err != nil { // handling error
		return err
	}
	defer file.Close()

	if ca, err := io.ReadAll(file); err == nil {
		cfg.RootCAs.AppendCertsFromPEM(ca)
	} else {
		return fmt.Errorf("certificate read failed. err :%v", err)
	}
	if !rmq.SSLDetails["TLS_ONLY_CA_CERT"].(bool) {
		if cert, err := tls.LoadX509KeyPair(rmq.SSLDetails["client-cert"].(string), rmq.SSLDetails["client-key"].(string)); err == nil {
			cfg.Certificates = append(cfg.Certificates, cert)
		}
	}
	cfg.InsecureSkipVerify = rmq.SSLDetails["TLS_INSECURE_SKIP_VERIFY"].(bool)

	conn, err := amqp.DialTLS(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", rmq.UserName, rmq.Password, rmq.Host, rmq.Port, rmq.VHost), cfg)
	if err != nil {
		//connection failed
		return fmt.Errorf("ssl failed, err :%v", err)
	}

	//store connection and channel
	rmq.Connection = conn
	return nil
}

/*
queue declare
*/
func (rmq *RabbitMQ) queueDeclare(qname string) (amqp.Queue, error) {

	rmqueue, err := rmq.Channel.QueueDeclare(
		qname,       //queue name
		rmq.Durable, //Durable
		false,       //delete when unused
		false,       //exclusive
		false,       //no waits
		nil,         //arguments
	)
	if err != nil {
		//queue declaration failed
		return rmqueue, fmt.Errorf("queue not declared, err :%v", err)
	}
	//queue declared
	fmt.Printf("Queue {%s} declared\n", rmqueue.Name)
	return rmqueue, nil
}

/*
Exchange declare and bind
*/
func (rmq *RabbitMQ) exchangeDeclare() error {

	err := rmq.Channel.ExchangeDeclare(
		rmq.Exchange, //Exchange Name
		"direct",     //Exchange Type
		rmq.Durable,  //Durable
		false,        //Delete When Unused
		false,        //internal
		false,        //No-Wait
		nil,          //Arguments
	)
	if err != nil {
		//queue declaration failed
		return fmt.Errorf("exchange declare failed, err :%v", err)
	}
	return nil
}

/*
disconnection function
*/
func (rmq *RabbitMQ) Disconnect() error {

	//check whether client active or not
	if !rmq.Connection.IsClosed() {
		//rabbitmq channel availability
		if rmq.Channel == nil {
			//close client
			rmq.Connection.Close()
		} else {
			//close channel
			err := rmq.Channel.Close()
			if err != nil {
				//channel closure failed
				return fmt.Errorf("disconnect channel failed, err : %s", err)
			}
			//disconnect
			err = rmq.Connection.Close()
			if err != nil {
				//disconnection failed
				return fmt.Errorf("disconnect client failed, err : %s", err)
			}
		}
	}
	return nil
}

/*
reconnection function
*/
func (rmq *RabbitMQ) Reconnect() error {
	rmq.Disconnect()
	var retry int
	var err error
	//retry until attempts are exhausted or connection is created
	for retry = 1; retry <= rmq.RetryCount; retry++ {
		//create connection

		if err = rmq.Connect(); err == nil {
			//connected
			break
		}
		fmt.Printf("RabbitMQReConnect failed. Retry count:%d", retry)
		//delay until next attempt
		time.Sleep(time.Duration(rmq.RetryDelay))
	}

	if retry > rmq.RetryCount && err != nil {
		return fmt.Errorf("reconnect failed, err: %v", err)
	}
	return nil
}

/*
publisher function
*/
func (rmq *RabbitMQ) Publisher(queuename string, message string) error {

	//declare queue to publish in
	queue, err := rmq.queueDeclare(queuename)
	if err != nil {
		return err
	}
	args := make(amqp.Table)
	args["x-max-priority"] = ""
	//bind queue to Exchange
	err = rmq.Channel.QueueBind(
		queue.Name,   //queue name
		queue.Name,   //routing key
		rmq.Exchange, //Exchange name
		false,        //no wait
		args,         //arguments
	)

	if err != nil {
		return fmt.Errorf("queue bind failed, err :%v", err)
	}

	//check connection
	if rmq.Connection.IsClosed() {
		if err := rmq.Reconnect(); err != nil {
			return fmt.Errorf("err: %v", err)
		}
	}

	//publishing initialization block
	body := []byte(message)
	msg := amqp.Publishing{
		ContentType:     "text/plain",
		ContentEncoding: "UTF-8",
		DeliveryMode:    amqp.Persistent,
		Body:            (body),
	}

	//publish message block
	err = rmq.Channel.PublishWithContext(
		context.Background(),
		rmq.Exchange, //Exchange
		queue.Name,   //Routing Key
		true,         //Mandatory
		false,        //Immediate
		msg,
	)

	//check if publish gave error
	if err != nil {
		return err
	}

	//get publishing acknowledgement
	for {
		if rmq.InterruptFlag {
			return errors.New("ctrl-c exit")
		}
		select {
		case confirm := <-rmq.NotifyConfirm:
			if confirm.Ack {
				fmt.Printf("Msg is Published {%s} in the Queue={%s}\n", message, queue.Name)
				return nil
			}
		case <-time.After(time.Duration(rmq.Timeout) * time.Second):
			fmt.Println("No acknowledgement yet")
			continue
		}
	}
}
