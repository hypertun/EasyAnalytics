package com.sqlwrapper.service;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLValidator {
    private static final Logger logger = LoggerFactory.getLogger(SQLValidator.class);
    private static final List<String> DANGEROUS_KEYWORDS = Arrays.asList(
        "drop", "alter", "truncate", "delete", "insert", "update", "create",
        "grant", "revoke", "exec", "execute", "shutdown", "kill", "commit", "rollback",
        "load_file", "into outfile", "into dumpfile", "sys_exec", "system", "eval"
    );
    
    private static final Pattern COMMENT_PATTERN = Pattern.compile("/\\*.*?\\*/|(--\\s.*?$)", Pattern.MULTILINE);
    
    public boolean isValid(String sql) {
        String cleanSql = removeComments(sql).toLowerCase().trim();

        // Check for dangerous keywords
        for (String keyword : DANGEROUS_KEYWORDS) {
            // Use word boundary to prevent partial matches (e.g., "select" should match, but "selective" shouldn't)
            if (cleanSql.matches(".*\\b" + keyword + "\\b.*")) {
                logger.warn("Dangerous keyword detected: {}", keyword);
                return false;
            }
        }

        // Must be a SELECT statement
        if (!cleanSql.startsWith("select")) {
            logger.warn("Query is not a SELECT statement");
            return false;
        }

        // Parse to verify it's valid SQL
        try {
            Statement statement = CCJSqlParserUtil.parse(cleanSql);
            if (!(statement instanceof Select)) {
                logger.warn("Parsed statement is not a Select");
                return false;
            }
            
            // Additional validation: check for subqueries that might be dangerous
            // (This would require more complex parsing logic)
            
            return true;
        } catch (JSQLParserException e) {
            logger.error("SQL parsing failed", e);
            return false;
        }
    }
    
    private String removeComments(String sql) {
        return COMMENT_PATTERN.matcher(sql).replaceAll("");
    }
}