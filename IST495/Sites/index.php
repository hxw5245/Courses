<?php 
include('inc/header.php');
?>
<title>Trips by Distance</title>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/css/bootstrap.min.css">
<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js"></script>
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.5/js/bootstrap.min.js"></script>
<script src="js/search.js"></script>
<?php include('inc/container.php');?>
<div class="container">
	<h2>Trips by Distance</h2>
	<br>
	<div class="row">
		<div class="form-group col-md-3">
			<input type="text" class="search form-control" id="keywords" name="keywords" placeholder="By State Postal Code or County Name">			
		</div>
		<div class="form-group col-md-2">
			<input type="button" class="btn btn-primary" value="Search" id="search" name="search" />
		</div>		
		<div class="form-group col-md-4">
			<select class="form-control" id="sortSearch">
			  <option value="">Sort By</option>
			  <option value="new">Newest</option>
			  <option value="asc">Ascending</option>
			  <option value="desc">Descending</option>
			  <option value="Processed">National</option>
			  <option value="Pending">State</option>
			  <option value="Cancelled">County</option>
			</select>
		</div>
	</div>
    <div class="loading-overlay" style="display: none;"><div class="overlay-content">Loading.....</div></div>
    <table class="table table-striped">
        <thead>
            <tr>
				<th>Level</th>
				<th>Date</th>
				<th>State Postal Code</th>
				<th>County Name</th>
				<th>Number of Trips</th>
				<th>Week</th>
				<th>Month</th>
            </tr>
        </thead>
        <tbody id="userData">		
			<?php			
			include 'Search.php';
			$search = new Search();
			$allOrders = $search->searchResult(array('order_by'=>'id DESC'));      
			if(!empty($allOrders)) {
				foreach($allOrders as $order) {
					$status = '';
					if($order["Level"] == 'National') {
						$status = 'btn-success';
					} else if($order["Level"] == 'State') {
						$status = 'btn-warning';
					} else if($order["Level"] == 'County') {
						$status = 'btn-danger';
					}
					echo '
					<tr>
					<td><button type="button" class="btn '.$status.' btn-xs">'.$order["Level"].'</button></td>
					<td>'.$order["Date"].'</td>
					<td>'.$order["State_Postal_Code"].'</td>
					<td>'.$order["County_Name"].'</td>
					<td>$'.$order["Number_of_Trips"].'</td>
					<td>'.$order["Week"].'</td>
					<td>'.$order["Month"].'</td>
					</tr>';
				}
			} else {
			?>            
				<tr><td colspan="6">No Result found...</td></tr>
			<?php } ?>
        </tbody>
    </table>	
</div>	
</div>	
<?php include('inc/footer.php');?>