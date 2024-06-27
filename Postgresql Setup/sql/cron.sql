SELECT cron.schedule('0 0 * * *', 'Select Update_Daily_Marketing_Data()');
SELECT cron.schedule('0 0 1 * *', 'Select Archived_Orders()');