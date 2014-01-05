<?php
/**
 *  NOTES:
 *   -variable length primitives: string always preceded by int16 for length, bytes always preceded by int32 for length
 *   -arrays: always preceded by int32 for length

 *   -int16|32|64 : always big endian
 */
namespace Zurau;

class Zurau
{
	const API_VERSION = 0;
	const CLIENT_NAME = 'php_zurau';

	public function __construct(array $config){
		$this->client_info = pack('n',strlen(self::CLIENT_NAME)).self::CLIENT_NAME;
		$this->config = $config;
	}

	public function metadata($topic = ''){
		$request = $this->format_metadata_request(array('topic'=>$topic));
		$fp = fsockopen($this->config['host'], $this->config['port'], $errno, $errstr, 5);
		fwrite($fp, $request);
		var_dump($this->parse_response($fp));
		fclose($fp);
	}

	public function send(){
		
	}

	public function fetch(){
		
	}

	public function offsets(){
		
	}

	public function offset_commit(){
		
	}

	public function offset_fetch(){

	}

	/**
	 *  0 - ProduceRequest
	 *  1 - FetchRequest
	 *  2 - OffsetRequest
	 *  3 - MetadataRequest
	 *  4 - LeaderAndIsrRequest
	 *  5 - StopReplicaRequest
	 *  6 - OffsetCommitRequest
	 *  7 - OffsetFetchRequest
	 */
	private function getApiKey($type){
		switch($type){
			case 'produce':
				return 0;
				break;
			case 'fetch':
				return 1;
				break;
			case 'offset':
				return 2;
				break;
			case 'metadata':
				return 3;
				break;
			case 'leader':
				return 4;
				break;
			case 'stopreplica':
				return 5;
				break;
			case 'offsetcommit':
				return 6;
				break;
			case 'offsetfetch':
				return 7;
				break;
		}
	}
	/**
	 * $params = array(
	 *  'size'=>int32
	 *  'key'=>int16
	 *  'version=>int16
	 *  'correlation'=>int32 (used to match requests w/ responses)
	 *  'client'=>string
	 *  'message'=>
	 * )
	 */
	private function format_request($type, array $params){
		$request = pack('n', $this->getApiKey($type));
		$request.= pack('n', self::API_VERSION);
		$request.= pack('N', 1); // $params['correlation'] - for now to make it simpler
		$request.= $this->client_info;
		$request.= $params['message'];
		$request = pack('N', strlen($request)).$request;

		return $request;
	}

	/**
	 * $params = array(
	 *  'topic'=>string
	 * )
	 */
	private function format_metadata_request(array $params){
		$message = pack('N', 1);
		$message.= pack('n', strlen($params['topic']));
		$message.= $params['topic'];
		$params['message'] = $message;

		return $this->format_request('metadata', $params);
	}

	/**
	 * return array(
	 *  'size'=>int32
	 *  'correlation'=>int32 (same value as passed with request)
	 *  'crc'=>int32
	 *  'magicbyte'=>int8
	 *  'attributes'=>int8
	 *  'key'=>bytes
	 *  'value'=>bytes
	 * )
	 */
	private function parse_response($fp){
		$size = $this->int32(fread($fp, 4));
		return $this->parse_metadata_response(fread($fp, $size));
	}

	private function parse_metadata_response($response){
		$offset = 0;
		$correlation_id = $this->int32($response, $offset);

		/* Brokers */
		$numBrokers = $this->int32($response, $offset);
		$brokers = array();
		while($numBrokers-- > 0){
			$tmp = array();
			$tmp['node'] = $this->int32($response, $offset);
			$hostlen = $this->int16($response, $offset);
			$tmp['host'] = substr($response,$offset,$hostlen);
			$offset+=$hostlen;
			$tmp['port'] = $this->int32($response, $offset);
			$brokers[] = $tmp;
		}

		/* TopicMetadata */
		$numSegments = $this->int32($response, $offset);
		$metadata = array();
		while($numSegments-- > 0){
			$tmp = array();
			$tmp['error_code'] = $this->int16($response, $offset);
			$namelen = $this->int16($response, $offset);
			$tmp['topic'] = substr($response, $offset, $namelen);
			$offset+=$namelen;

			$p = array();
			$partlen = $this->int32($response, $offset);
			$parts = array();
			while($partlen-- > 0){
				$tmp2 = array();
				$tmp2['error_code'] = $this->int16($response, $offset);
				$tmp2['id'] = $this->int32($response, $offset);
				$tmp2['leader'] = $this->int32($response, $offset);
				$numreplicas = $this->int32($response, $offset);
				$replicas = array();
				while($numreplicas-- > 0){
					$replicas[] = $this->int32($response, $offset);
				}
				$tmp2['replicas'] = $replicas;

				$numisrs = $this->int32($response, $offset);
				$isr = array();
				while($numisrs-- > 0){
					$isr[] = $this->int32($response, $offset);
				}
				$tmp2['isr'] = $isr;
				$parts[] = $tmp2;
			}
			$tmp['partitions'] = $parts;
			$metadata[] = $tmp;
		}
		return array(
			'brokers'=>$brokers,
			'metadata'=>$metadata
		);
	}

	private function int16($response, &$offset){
		$val = unpack('n', substr($response, $offset, 2));
		$offset += 2;
		return $val[1];
	}

	private function int32($response, &$offset = 0){
		$val = unpack('N', substr($response, $offset, 4));
		$offset += 4;
		return $val[1];
	}

	/**
	 * $params = array(
	 *  'NodeId'=>int32
	 *  'Host'=>string
	 *  'Port'=>int32
	 *  'TopicErrorCode'=>int16
	 *  'PartitionErrorCode'=>int16
	 *  'PartitionId'=>int32
	 *  'Leader'=>int32
	 *  'Replicas'=>[int32]
	 *  'Isr'=>[int32]
	 * )
	 */
	private function metadata_response(){
		
	}

	/**
	 *  0 - NoError : No error
	 * -1 - Unknown : unexpected server error
	 *  1 - OffsetOutOfRange : requested offset is outside range of offsets maintained by the server for the given topic / partition
	 *  2 - InvalidMessage : a message contents does not match its CRC
	 *  3 - UnknownTopicOrPartition : request is for a topic or partition that does not exist on this broker
	 *  4 - InvalidMessageSize : message has a negative size
	 *  5 - LeaderNotAvailable : there is currently no leader for this partition and hence is unavailable for writes
	 *  6 - NotLeaderForPartition : attempted to send message to a replica that is not the leader for the partition
	 *  7 - RequestTimedOut : request exceeds user-specified time limit in the request
	 *  8 - BrokerNotAvailable : only used internally by intra-cluster broker communication
	 *  9 - ReplicaNotAvailable : 
	 * 10 - MessageSizeTooLarge : client attempted to produce a message larger than configured maximum message size on server
	 * 11 - StaleControllerEpochCode : ???
	 * 12 - OffsetMetadataTooLargeCode : If you specify a string larger than configured maximum for offset metadata
	 */
	private function error_codes(){

	}
}
