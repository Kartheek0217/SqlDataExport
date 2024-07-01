package com.migration.dbManager.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.migration.dbManager.mysql.MysqlOperations;

@RestController
@RequestMapping("/migration")
public class DataMigration {

	private static final Logger LOG = LoggerFactory.getLogger(DataMigration.class);

	@Autowired
	private MysqlOperations mysqlOperations;

	@GetMapping("/hi")
	public String hi() {

		LOG.info("entered method");

		String query = "select * from analyzer";

		String[] headers = { "id", "created_by", "created_on", "modified_by", "modified_on", "address_1", "address_2",
				"apex_id", "branches", "building_type", "coops_id", "country_id", "covered_vil_count", "cut_off_date",
				"dccb_id", "dccbbr_id", "dist_to_dccbbr", "district_id", "establishment_date", "fhr_approval_status",
				"fhr_copy_path", "fhr_remarks", "gst_number", "internal_pacs_code", "isbranch", "mandal_id",
				"nabard_pacs_code", "name", "num_of_txn_per_day", "pacs_code", "pacs_sequence_number", "pacs_status",
				"pan_number", "parent_pacs_id", "pin_code", "reg_date", "reg_number", "selected_by_nabard",
				"short_name", "society_code", "state_id", "status", "type_of_society_id", "village_id" };

		mysqlOperations.exportSourceListToCsv(query, headers, "./myfile.csv");

		return null;
	}
}