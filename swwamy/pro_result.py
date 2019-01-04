def main_run(self):
	# self.validate()
	if not self.mnemonics:
		self.mnemonics = self.get_mnemonic()
	elif self.add_mnemonics:
		self.mnemonics = self.mnemonics + self.get_mnemonic()
	if self.securities:
		if len(self.securities) > 10000:

			payload = self.gen_request_object(self.securities, self.mnemonics)
			response = self.post_to_bt(payload)
			if response['request_status'] == self.success and response['data_file_path']:
				dst_file_path = self.copy_file(response['data_file_path'])
				if self.modify_header:
					self.change_header(dst_file_path)
				self.ret_code = ResultCode.SUCCESS_ITEM_PROCESSED.value
			else:
				sys.exit('Unexpected response {}. Review request object \nrequest_object:{}'.format(
					response['request_status'], payload))
		else:
			n = 500
      			chunk_total = list(divide_chunks(self.securities, n))
      			response_list = []
      			for total in chunk_total:
          			payload = self.gen_request_object(total, self.mnemonics)
			    	response = self.post_to_bt(payload)
          			response_list.extend(response)
			# here we need to change response as response_list or vice-versa , depends on getting the result 
			if response['request_status'] == self.success and response['data_file_path']:
				dst_file_path = self.copy_file(response['data_file_path'])
				if self.modify_header:
					self.change_header(dst_file_path)
				self.ret_code = ResultCode.SUCCESS_ITEM_PROCESSED.value
			else:
				sys.exit('Unexpected response {}. Review request object \nrequest_object:{}'.format(
					response['request_status'], payload))

	yield self.ret_code


def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]
