type Preset = { prompt: string; expected: Record<string, unknown> };

const fraudTypes = ["off-register payment", "extortion", "personal discount", "refund fraud", "product substitution"];
const fraudExamples = "Look for off-register payments to an employee, extortion, unauthorized personal discounts, fraudulent refunds, and deliberate product substitution.";

export function englishReadyPreset(grouped: boolean): Preset {
  if (grouped) {
    return {
      prompt: [
        "You are assessing whether customer feedback about one store indicates employee fraud.",
        fraudExamples,
        "Count only reviews that explicitly describe a scheme. Exclude poor service, defects without intent, and ambiguous complaints.",
        "Set confirmed_cases to the count and share_percent to the percentage of all reviews, rounded to one decimal place.",
        "List only fraud_types that occur in this group. Return an empty array if you find none.",
        "Set risk to low for 0–1 cases or a share below 10%, medium for 2–4 cases and a 10–20% share, and high for 5+ cases, a share above 20%, or any extortion. For conflicting criteria use the lower level, unless extortion occurred.",
        "In summary, state the count, share and schemes in 2–3 English sentences. Add recommendations only at medium or high risk.",
      ].join("\n"),
      expected: {
        confirmed_cases: { type: "integer", min: 0 },
        share_percent: { type: "number", min: 0, max: 100 },
        fraud_types: { type: "array", items: { type: "string" } },
        risk: { type: "enum", values: ["low", "medium", "high"] },
        summary: { type: "string", max_length: 400 },
      },
    };
  }
  return {
    prompt: [
      "Classify a customer review by its main subject. Use fraud only for explicit evidence of employee fraud.",
      fraudExamples,
      "Choose category from fraud, poor service, defective product, positive, or neutral.",
      "Set fraud_type to a specific scheme, other fraud, or no fraud. Treat bad service and product defects alone as non-fraud.",
      "Give confidence from 0 to 1, with a value below 0.6 for ambiguous wording. Summarize the evidence in English in 1–2 sentences.",
    ].join("\n"),
    expected: {
      category: { type: "enum", values: ["fraud", "poor service", "defective product", "positive", "neutral"] },
      fraud_type: { type: "enum", values: ["no fraud", ...fraudTypes, "other fraud"] },
      confidence: { type: "number", min: 0, max: 1 },
      summary: { type: "string", max_length: 240 },
    },
  };
}
