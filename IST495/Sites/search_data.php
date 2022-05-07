<?php
include 'Search.php';
$search = new Search();
$sqlConditions = array();
if(!empty($_POST['type']) && (!empty($_POST['keywords']) || !empty($_POST['sortValue']))){
    if($_POST['type'] == 'search'){
        $sqlConditions['search'] = array('County_Name'=>$_POST['keywords'],'State_Postal_Code'=>$_POST['keywords'],'Number_of_Trips'=>$_POST['keywords']);
        $sqlConditions['order_by'] = 'id DESC';
    }elseif($_POST['type'] == 'sort'){
		if($_POST['keywords']) {
			$sqlConditions['search'] = array('County_Name'=>$_POST['keywords'],'State_Postal_Code'=>$_POST['keywords'],'Number_of_Trips'=>$_POST['keywords']);
		}
        $sortValue = $_POST['sortValue'];
        $sortArribute = array(
            'new' => array(
                'order_by' => 'Date DESC'
            ),
            'asc'=>array(
                'order_by'=>'Number_of_Trips ASC'
            ),
            'desc'=>array(
                'order_by'=>'Number_of_Trips DESC'
            ),
            'Processed'=>array(
                'where'=>array('Level'=>'National')
            ),
            'Pending'=>array(
                'where'=>array('Level'=>'State')
            ),
			'Cancelled'=>array(
                'where'=>array('Level'=>'County')
            )
        );
        $sortKey = key($sortArribute[$sortValue]);
        $sqlConditions[$sortKey] = $sortArribute[$sortValue][$sortKey];
    }
}else{
    $sqlConditions['order_by'] = 'id DESC';
}
$orders = $search->searchResult($sqlConditions);
if(!empty($orders)){    
	foreach($orders as $order){
		$status = '';
		if($order["Level"] == 'National') {
			$status = 'btn-success';
		} else if($order["Level"] == 'State') {
			$status = 'btn-warning';
		} else if($order["Level"] == 'County') {
			$status = 'btn-danger';
		}
		echo '<tr>';
		echo '<td><button type="button" class="btn '.$status.' btn-xs">'.$order["Level"].'</button></td>';
		echo '<td>'.$order['Date'].'</td>';
		echo '<td>'.$order['State_Postal_Code'].'</td>';
		echo '<td>'.$order['County_Name'].'</td>';
		echo '<td>'.$order['Number_of_Trips'].'</td>';
		echo '<td>'.$order['Week'].'</td>';
		echo '<td>'.$order['Month'].'</td>';
		echo '</tr>';
	}
}else{
    echo '<tr><td colspan="6">No Result found...</td></tr>';
}
exit;