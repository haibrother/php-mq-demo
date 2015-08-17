<?php

class CALLWorker extends Worker {

	protected  static $client;
    
	public function __construct() {

	}
	public function run(){
    
         self::$client = '对象';

	}
    
	protected function getConnection(){ 
            return self::$client;
    }
    

}

/* the collectable class implements machinery for Pool::collect */
class Sendsms extends Stackable {
    
	public function __construct($row) {
		$this->data = $row;
        
	}

	public function run() 
    {
        //获取对象
       $client = $this->worker->getConnection();
       write_log('mq获取到的数据：'.$this->data.',字符：'.$client);
	}
}

class Client {
    static $pool;

    public static function main() {
        
        self::$pool = new Pool(MAX_CONCURRENCY_JOB, \CALLWorker::class, []);
        $conn_args = require('mq.config.php');
        
        $e_name = 'e_linvo'; //交换机名
        $q_name = 'q_linvo'; //队列名
        $k_route = 'key_1'; //路由key
        
        //创建连接和channel
        $conn = new AMQPConnection($conn_args);
        if (!$conn->connect()) {
            write_log('mq.hx9999.com 无法连接上');
            return;
        }
        $channel = new AMQPChannel($conn);

        //创建交换机
        $ex = new AMQPExchange($channel);
        $ex->setName($e_name);
        $ex->setType(AMQP_EX_TYPE_DIRECT); //direct类型
        $ex->setFlags(AMQP_DURABLE); //持久化
        
        //创建队列
        $q = new AMQPQueue($channel);
        $q->setName($q_name);
        $q->setFlags(AMQP_DURABLE); //持久化
        while(True){
             $q->consume(function($envelope, $queue)
            {
                $msg = $envelope->getBody();
                
                $queue->ack($envelope->getDeliveryTag()); //手动发送ACK应答
                self::$pool->submit(new Sendsms($msg));
            });
        }
        
        self::$pool->shutdown();
        $conn->disconnect();
        
        //睡眠
        sleep(5);
        
    }
}



class Example {
	/* config */
	const LISTEN = "0.0.0.0";
	const MAXCONN = 100;
	const pidfile = 'mq';
	const uid	= 80;
	const gid	= 80;
	
	protected $pool = NULL;
	protected $zmq = NULL;
	public function __construct() {
		$this->pidfile = '/var/run/'.self::pidfile.'.pid';
	}
	private function daemon(){
		if (file_exists($this->pidfile)) {
			echo "The file $this->pidfile exists.\n";
			exit();
		}
		
		$pid = pcntl_fork();
		if ($pid == -1) {
			 die('could not fork');
		} else if ($pid) {
			 // we are the parent
			 //pcntl_wait($status); //Protect against Zombie children
			exit($pid);
		} else {
			// we are the child
			file_put_contents($this->pidfile, getmypid());
			posix_setuid(self::uid);
			posix_setgid(self::gid);
			return(getmypid());
		}
	}
	private function start(){
		$pid = $this->daemon();
        write_log('mq队列开始');
        while( true ){
            Client::main();
        }
        write_log('mq队列结束');
		
	}
	private function stop(){

		if (file_exists($this->pidfile)) {
			$pid = file_get_contents($this->pidfile);
			posix_kill($pid, 9); 
			unlink($this->pidfile);
		}
	}
	private function help($proc){
		printf("%s start | stop | help \n", $proc);
	}
	public function main($argv){
		if(count($argv) < 2){
			printf("please input help parameter\n");
			exit();
		}
		if($argv[1] === 'stop'){
			$this->stop();
		}else if($argv[1] === 'start'){
			$this->start();
		}else{
			$this->help($argv[0]);
		}
	}
}

/*
 *写日志
 **/
 function write_log($msg)
 {
    $msg = sprintf("%s,%s\n", date('Y-m-d H:i:s'), $msg);
    file_put_contents('/www/hx9999.com/log.hx9999.com/mq.' . date("Y-m-d") . '.log', $msg, FILE_APPEND);
 }
 
// 定义可以同时执行的进程数量
define('MAX_CONCURRENCY_JOB', 20);
$cgse = new Example();
$cgse->main($argv);

?>
