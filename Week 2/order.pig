/* load the file orders.csv */
ordersCSV = LOAD '/user/maria_dev/diplomacy/orders.csv'
    USING PigStorage(',') 
        AS
            (game_id:chararray,
            unit_id:chararray,
            unit_order:chararray,
            location:chararray,
            target:chararray,
            target_dest:chararray,
            success:chararray,
            reason:chararray,
            turn_num:chararray);
	
    --DUMP ordersCSV
    /*limitedOrders = LIMIT ordersCSV 5;
    DUMP limitedOrders;*/
 	--targetLocation == "Holland";
 	-- filter order by target Holland
 	filterResult = FILTER ordersCSV BY target == 'Holland';
 	--DUMP filterResult;
    
    --group by location
    groupTheList = GROUP filterResult  BY (location, target);
    
    
    DUMP groupTheList;
    
