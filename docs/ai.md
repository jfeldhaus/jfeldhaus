# AI

## Data Analysis

The screenshot below is from an application I designed and built that compares the results of a long running stress test executed against two different versions of a database. The graphs show the progression of processes over the duration of the test.

Below the graphs, an AI agent (Codex) that I integrated into the tool automatically compares the output for each corresponding process and describes any significant differences. In this case the agent correctly identifies that two processes failed due to missing shared libraries. This combination of structured data visualization and AI analysis of text is very effective in detecting problems.

![Side-by-side workload timeline charts comparing two database builds, followed by AI-generated bullet points summarizing the differences](../images/ai-compare.jpg)

Both flagged job pairs failed in the older build with a missing shared library error, while the newer build completed successfully with measurable throughput (1,229 TPS and 307 TPS) — exactly the kind of regression that's easy to miss scanning raw logs manually but immediately visible once summarized.

## Functional Testing

The SQL queries below demonstrate a bug where the first query returns an incorrect result. The only difference between the queries is that the second one uses an optimizer hint. Optimizer settings should not alter query results. I built the query-result validation program, using AI to generate the test cases, that detected this bug. The development of test software is a powerful use case of AI.

```         

SQL> SELECT jt.dv_leg_number, jt.dv_leg_type,
  2         l.leg_number     AS rel_leg_number,
  3         l.leg_type       AS rel_leg_type
  4  FROM  (SELECT data FROM trade_dv
  5         WHERE  json_value(data, '$._id' RETURNING NUMBER) = 1) dv
  6  CROSS JOIN JSON_TABLE(dv.data, '$.legs[*]' COLUMNS (
  7      dv_leg_number NUMBER       PATH '$.legNumber',
  8      dv_leg_type   VARCHAR2(20) PATH '$.legType')) jt
  9  JOIN trade_legs l ON l.trade_id = 1 AND l.leg_number = jt.dv_leg_number;

0 rows selected. 


SQL> SELECT /*+ NO_MERGE(dv) */
  2         jt.dv_leg_number, jt.dv_leg_type,
  3         l.leg_number     AS rel_leg_number,
  4         l.leg_type       AS rel_leg_type
  5  FROM  (SELECT data FROM trade_dv
  6         WHERE  json_value(data, '$._id' RETURNING NUMBER) = 1) dv
  7  CROSS JOIN JSON_TABLE(dv.data, '$.legs[*]' COLUMNS (
  8      dv_leg_number NUMBER       PATH '$.legNumber',
  9      dv_leg_type   VARCHAR2(20) PATH '$.legType')) jt
 10  JOIN trade_legs l ON l.trade_id = 1 AND l.leg_number = jt.dv_leg_number;

DV_LEG_NUMBER DV_LEG_TYPE          REL_LEG_NUMBER REL_LEG_TYPE        
------------- -------------------- -------------- --------------------
            1 LONG                              1 LONG                

1 row selected. 
```

The only difference between the two queries is the `NO_MERGE` hint, yet one returns 0 rows and the other returns the correct row — a genuine optimizer correctness bug, not a performance issue.

## Document Generation

The document snippet below was created using a program I built that combines and summarizes information from multiple disparate data sources. In this case the document describes a complex test configuration. After the program gathers the information, it uses AI to generate a concise summary of what the test does and suggest how the test can be improved.

![Generated document table summarizing a stress test configuration file with a bulleted workload summary](../images/stress-doc.png)

The generated summary correctly names every tool used across the workload and proposes a concrete next test, rather than just describing what already ran.

------------------------------------------------------------------------
