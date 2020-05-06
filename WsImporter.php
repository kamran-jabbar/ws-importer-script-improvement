<?php

/**
 * Class WsImporter
 */
class WsImporter
{
    /**
     * XML type constants
     */
    const XML_KEY_NETWORK = 'network';
    const XML_KEY_EVENT = 'event';
    const XML_KEY_SERVICE = 'service';

    /**
     * Database Connection Setting Constants
     */
    const DB_DSN = 'mysql:host=ws-database;dbname=ws-import';
    const DB_USERNAME = 'root';
    const DB_PASSWORD = 'hunter2';

    /**
     * DB level constants
     */
    const SHOW_TYPE = 'other';
    const TABLE_NAME_SERVICE_LIVE_TV_PROGRAM = 'service_livetv_program';
    const TABLE_NAME_SERVICE_LIVE_TV_CHANNEL = 'service_livetv_channel';
    const TABLE_NAME_SERVICE_LIVE_TV_SCHEDULE = 'service_livetv_schedule';

    /**
     * @var SimpleXMLElement
     */
    public $xml;

    /**
     * @var PDO
     */
    public $pdo;

    /**
     * WsImporter constructor.
     */
    function __construct()
    {
        $this->xml = $this->getXml();
        $this->pdo = $this->getDatabaseConnection();
    }

    /**
     * @return SimpleXMLElement
     */
    private function getXml()
    {
        try {
            return simplexml_load_string(file_get_contents('/code/kuivuri.xml'));
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * @return PDO
     */
    private function getDatabaseConnection()
    {
        try {
            return new PDO(self::DB_DSN, self::DB_USERNAME, self::DB_PASSWORD);
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * @param SimpleXMLElement $xml
     * @param string $xmlKey
     * @return SimpleXMLElement
     */
    private function parseXml($xml, $xmlKey)
    {
        try {
            return $xml->{$xmlKey};
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * Use to process the required network.
     */
    public function processNetwork()
    {
        try {
            $this->truncateTables();//this is just for testing, remove it if you want to run the script only once.
            $writtenSchedules = 0;
            $networksXml = $this->parseXml($this->xml, self::XML_KEY_NETWORK);

            foreach ($networksXml as $network) {
                $serviceXml = $this->parseXml($network, self::XML_KEY_SERVICE);
                $this->processService($serviceXml, $writtenSchedules);
            }
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * Use to process the required service.
     * @param SimpleXMLElement $serviceXml
     * @param int $writtenSchedules
     */
    private function processService($serviceXml, $writtenSchedules)
    {
        try {
            foreach ($serviceXml as $service) {
                $serviceId = $service['id'];
                $channel = $this->mysqlSelectQuery(self::TABLE_NAME_SERVICE_LIVE_TV_CHANNEL, 'id',
                    ['WHERE source_id = "%s"', [$serviceId]]);

                if ($channel === false) {
                    continue; // If channel doesn't exist in DB, ignore the data
                }
                $eventXml = $this->parseXml($service, self::XML_KEY_EVENT);
                $this->processEvent($eventXml, $channel, $writtenSchedules);
            }
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * Use to process the required event.
     * @param SimpleXMLElement $eventXml
     * @param array $channel
     * @param int $writtenSchedules
     */
    private function processEvent($eventXml, $channel, $writtenSchedules)
    {
        try{
            foreach ($eventXml as $event) {
                $eventId = $event['id'];
                $eventStart = $event['start_time'];
                $eventDuration = $event['duration'];
                $program = $event->language[0]->short_event;
                $programLanguage = $program['language'];
                $programTitle = $program['name'];

                list($durationSeconds, $startTimestamp) =  $this->timeFormatter($eventStart, $eventDuration);

                $dataProgram = [
                    'ext_program_id' => $eventId,
                    'show_type' => self::SHOW_TYPE,
                    'long_title' => $programTitle,
                    'duration' => $durationSeconds,
                    'iso_2_lang' => $programLanguage
                ];
                // Write program
                $this->mysqlInsertQuery(self::TABLE_NAME_SERVICE_LIVE_TV_PROGRAM, $dataProgram);

                // Fetch inserted program
                $dbProgram = $this->mysqlSelectQuery(self::TABLE_NAME_SERVICE_LIVE_TV_PROGRAM, 'id',
                    ['WHERE ext_program_id = "%s"', [$eventId]]);

                $dataSchedule = [
                    'ext_schedule_id' => $eventId,
                    'channel_id' => $channel['id'],
                    'start_time' => $startTimestamp,
                    'end_time' => $startTimestamp + $durationSeconds,
                    'run_time' => $durationSeconds,
                    'program_id' => $dbProgram['id']
                ];
                // Write schedule event
                $this->mysqlInsertQuery(self::TABLE_NAME_SERVICE_LIVE_TV_SCHEDULE, $dataSchedule);
                echo sprintf("Written schedules: %d" . PHP_EOL, $writtenSchedules++);
            }
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * Use to select values from database table
     * @param string $tableName
     * @param string $selectColumn
     * @param array $where
     * @return array
     */
    public function mysqlSelectQuery($tableName, $selectColumn, $where = [])
    {
        try {
            if (!empty($where)) {
                // @todo: implement prepare here to avoid sql injection
                return $this->pdo->query(sprintf("SELECT $selectColumn FROM $tableName $where[0]",
                    implode(',', $where[1])))
                    ->fetch();
            }

            return $this->pdo->query(sprintf("SELECT $selectColumn FROM $tableName"))
                ->fetch();
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * Use to insert records in database with dynamic parameters.
     * @param string $table
     * @param array $data
     * @return bool
     */
    public function mysqlInsertQuery($table, $data)
    {
        $column = implode(',', array_keys($data));
        $values = array_values($data);
        $places = implode(', ', array_fill(0, count(array_keys($data)), '?'));

        try {
            $stmt = $this->pdo->prepare("INSERT INTO $table ($column) VALUES ($places)");

            if($stmt->execute($values)) {
                return true;
            }

            return false;
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * Use to log errors and exceptions
     * @param string $errorType
     * @param $errorContent
     */
    public function logger($errorType, $errorContent)
    {
        // @todo: implement logger to write errors and exceptions in some file
    }


    /**
     * Truncate tables to avoid duplicate errors when testing the script
     * @return bool
     */
    private function truncateTables()
    {
        try {
            $sql = sprintf(
                "SET FOREIGN_KEY_CHECKS = 0;
                    TRUNCATE TABLE `service_livetv_program`;
                    TRUNCATE TABLE `service_livetv_schedule`;"
            );

            if (!$this->pdo->query($sql)) {
                print_r($this->pdo->errorInfo());
                die;
            }
            // @todo: log message using logger function logger()
            echo 'All required tables truncated.';

            return true;
        } catch (Exception $e) {
            // @todo: log exception using logger function logger()
            echo 'Exception Message: ' . $e->getMessage();
            die;
        }
    }

    /**
     * Use to format the data time.
     * @param $eventStart
     * @param $eventDuration
     * @return array
     */
    public function timeFormatter($eventStart, $eventDuration)
    {
        list($hours, $minutes, $seconds) = explode(':', $eventDuration);
        $durationSeconds = $hours * 60 * 60 + $minutes * 60 + $seconds;
        $startTimestamp = DateTime::createFromFormat('y/m/d H:i:s', $eventStart)->getTimestamp();

        return [$durationSeconds, $startTimestamp];
    }
}

$importerClass = new WsImporter();
$importerClass->processNetwork();
?>