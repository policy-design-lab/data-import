import json
import random

us_state_abbreviations = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming'
}

if __name__ == '__main__':
    state_data_list = []

    for state in us_state_abbreviations:
        total_premium_subsidy = random.randrange(407519482, 4407519482)
        total_premium = random.randrange(total_premium_subsidy, 6550969070)
        total_indemnities = random.randrange((total_premium - total_premium_subsidy), 9332182396)

        state_data = {
            "state": state,
            "programs": [
                {
                    "programName": "Crop Insurance",
                    "totalIndemnitiesInDollars": total_indemnities,
                    "totalPremiumInDollars": total_premium,
                    "totalPremiumSubsidyInDollars": total_premium_subsidy,
                    "totalFarmerPaidPremiumInDollars": (total_premium - total_premium_subsidy),
                    "totalNetFarmerBenefitInDollars": total_indemnities - (total_premium - total_premium_subsidy),
                    "totalPoliciesEarningPremium": random.randrange(5607, 365607),
                    "totalLiabilitiesInDollars": random.randrange(18479642239, 28479642239),
                    "averageLossRatio": round(random.uniform(0.6, 1.5), 2),
                    "subPrograms": [
                    ]
                }
            ]
        }
        state_data_list.append(state_data)
    state_distribution_data = {
        "2018-2022": state_data_list
    }

    with open("crop_insurance_state_distribution_data.json", "w") as dist_file:
        json.dump(state_distribution_data, dist_file, indent=2)
