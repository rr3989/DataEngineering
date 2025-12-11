SELECT * FROM `vibrant-mantis-289406.trade_analysis.rejected_trades_audit` LIMIT 1000;

select count(*),client_id from `vibrant-mantis-289406.trade_analysis.approved_trades_validated` group by client_id;

select count(*),symbol from `vibrant-mantis-289406.trade_analysis.approved_trades_validated` group by symbol
order by count(*) desc;

select count(*), rejection_reason from `vibrant-mantis-289406.trade_analysis.rejected_trades_audit` group by rejection_reason;

select count(*), symbol from `vibrant-mantis-289406.trade_analysis.rejected_trades_audit` group by symbol order by count(*) desc;
