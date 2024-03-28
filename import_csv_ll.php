<?php

/**
 * Basic php script to import csv rows into DB,
 * focuses on two tables the cz_customers and cz_bookings table
 */


date_default_timezone_set('Europe/Stockholm');

 // Capture start time, gets the current time in microseconds as a float(more precise)
$startTime = microtime(true);


// Echoes the current time of when script started
echo "Script started at: " . date("Y-m-d H:i:s") . "\n";

// Set maximum execution time to unlimited, because we are expecting millions of lines of data
set_time_limit(0);

//DB info (had to use ' ' on password , otherwise php treats it like a variable, I could use \ to escape the $ sign but I chose ')
//also i needed to use 2 $mysqli because i have to prepare statements after one another
//                     //host       //user   //password    //db name

$mysqli = new mysqli("lol", "lol", 'lol', "lol");
$mysqli2 = new mysqli("lol", "lol", 'lol', "lol");

//chosen file for import
$csvFilePath = "larsson_bookings_cleaned.csv";
$file = fopen($csvFilePath, "r");

if ($file === FALSE) {
    echo "\nFailed to open file";
    exit;
}
//first sql statement to handle cz_customers table
$customerStmt = $mysqli->prepare("INSERT INTO cz_customers (customer_id, 
MobilePhoneNumber, 
PhoneNumber, 
FirstName, 
LastName, 
EmailAdress) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE customer_id=customer_id");

if ($customerStmt === false) {
    die("Failed to prepare customerStmt: " . $mysqli->error);
}

//second sql statement to handle cz_bookings table
$bookingStmt = $mysqli2->prepare("INSERT INTO cz_bookings (booking_id, 
LocationId, 
LocationName,
PersonId,
PersonName,
ServiceId,
ServiceName,
BookingPrice,
BookingStartDate,     
customer_id,
EventCreated, 
BookedOnline, 
Cancelled, 
FirstBooking) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

if ($bookingStmt === false) {
    die("Failed to prepare bookingStmt: " . $mysqli->error);
}

// var_dump($customerStmt, "customerstatement");
// var_dump($bookingStmt, "bookingstatement");

// if ($customerStmt === FALSE || $bookingStmt === FALSE) {
//     echo "\nFailed to prepare statement";
//     exit;
// }


// Start transaction
$mysqli->begin_transaction();


$rowCount = 0;
$errorCount = 0;
//here i set the delimiter for the rows to use ; and the 0 represents to continue for all rows
fgetcsv($file, 0, ';'); // Skip header row

//here i set the delimiter for the rows to use ; and the 0 represents to continue for all rows
while (($row = fgetcsv($file, 0, ';')) !== FALSE) {
    
    if(empty($row)) {
        echo "brake at " . $rowCount;
        break;
    }
    $rowCount++;

   

    // Extract customer data from the row
    $customerData = [
        $row[9],  // customer_id
        $row[10], // MobilePhoneNumber
        $row[11], // PhoneNumber
        $row[12], // FirstName
        $row[13], // LastName
        $row[14]  // EmailAddress
    ];

    // Insert customer data
    if (!$customerStmt->bind_param("ssssss", ...$customerData)) {
        echo "\nBind failed for customer row $rowCount: " . $customerStmt->error;
        $errorCount++;
        continue;
    }

    if (!$customerStmt->execute()) {
        echo "\nExecute failed for customer row $rowCount: " . $customerStmt->error;
        $errorCount++;
        continue;
    }

    // Extract booking data from the row
    $bookingData = [
        $row[0],  // booking_id
        $row[1],  // LocationId
        $row[2],  // LocationName
        $row[3],  // PersonId 
        $row[4],  // PersonName
        $row[5],  // ServiceId
        $row[6],  // ServiceName
        $row[7],  // BookingPrice
        $row[8],  // BookingStartDate
        $row[9],  // customer_id(same as in customerData)
        $row[15], // EventCreated
        $row[16], // BookedOnline
        $row[17], // Cancelled
        $row[18]  // FirstBooking
    ];

    // Insert booking data
    if (!$bookingStmt->bind_param("sssssssdsssiii", ...$bookingData)) {
        echo "\nBind failed for booking row $rowCount: " . $bookingStmt->error;
        $errorCount++;
        continue;
    }

  
    if (!$bookingStmt->execute()) {
        echo "\nExecute failed for booking row $rowCount: " . $bookingStmt->error;
        $errorCount++;
        continue;
    }
    
}


$endTime = microtime(true); // Capture end time
$executionTime = $endTime - $startTime; // Calculate the difference in seconds(total duration of script execution)

// Convert execution time to hours, minutes, and seconds, rounds down because i use floor
$hours = floor($executionTime / 3600);
$minutes = floor(($executionTime - ($hours * 3600)) / 60);
$seconds = floor($executionTime % 60);


// If there were no errors, commit the transaction
if ($errorCount === 0) {
    $mysqli->commit();
    echo "\nScript ended successfully after: $hours hours:, $minutes minutes:, and $seconds seconds:.";
} else {
    $mysqli->rollback();
    echo "\nScript ended with $errorCount errors:";
}

// Close the prepared statements and the file
$customerStmt->close();
$bookingStmt->close();
fclose($file);

?>