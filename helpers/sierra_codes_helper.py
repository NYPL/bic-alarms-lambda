def sierra_redshift_count_mismatch_alarm(self, sierra_count, 
                                             total_redshift_count):
        if sierra_count != total_redshift_count:
            self.logger.error((
                'Number of Sierra itype codes does not match number of '
                'Redshift itype codes: {sierra_count} Sierra codes and '
                '{redshift_count} Redshift codes')
                .format(sierra_count=sierra_count,
                        redshift_count=total_redshift_count))
            
def redshift_duplicate_code_alarm(self, total_redshift_count, 
                                    distinct_redshift_count):
    if total_redshift_count != distinct_redshift_count:
        self.logger.error((
            'Duplicate itype codes found in Redshift: {total_count} total '
            'active itype codes but only {distinct_count} distinct active '
            'itype codes').format(total_count=total_redshift_count,
                                distinct_count=distinct_redshift_count))

def null_code_alarm(self, null_codes):
    if self.run_added_tests and len(null_codes) > 0:
        self.logger.error(
            'The following itype_codes have a null value for one of their '
            'inferred columns: {codes}'.format(codes=null_codes))