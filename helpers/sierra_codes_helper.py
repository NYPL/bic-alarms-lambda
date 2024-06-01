def sierra_redshift_count_mismatch_alarm(self, code_type,
                                         sierra_count, total_redshift_count):
        if sierra_count != total_redshift_count:
            self.logger.error((
                'Number of Sierra {code_type} codes does not match number of '
                'Redshift {code_type} codes: {sierra_count} Sierra codes and '
                '{redshift_count} Redshift codes')
                .format(code_type=code_type,
                        sierra_count=sierra_count,
                        redshift_count=total_redshift_count))
            
def redshift_duplicate_code_alarm(self, code_type, 
                                  total_redshift_count, distinct_redshift_count):
    if total_redshift_count != distinct_redshift_count:
        self.logger.error((
            'Duplicate {code_type} codes found in Redshift: {total_count} '
            'total active {code_type} codes but only {distinct_count} '
            'distinct active {code_type} codes').format(code_type=code_type, 
                                  total_count=total_redshift_count,
                                  distinct_count=distinct_redshift_count))

def null_code_alarm(self, code_type, null_codes):
    if self.run_added_tests and len(null_codes) > 0:
        self.logger.error(
            'The following {code_type} have a null value for one of their '
            'inferred columns: {codes}'.format(code_type=code_type, codes=null_codes))