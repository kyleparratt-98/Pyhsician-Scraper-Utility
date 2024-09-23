import { pool } from "../../common/db_config.js";
import axios from "axios";
import { usStates } from "../../common/constants.js";
import { ConsoleLogger } from "../../common/logger.js";

const logger = new ConsoleLogger();
const args = process.argv.slice(2);
const argValue = args.find(arg => arg.startsWith('--arg='));
let specialty;

if (argValue) {
  specialty = argValue.split('=')[1];
  logger.logPretty('[root] Starting ingestion for specialty:', specialty);
} else {
  logger.logError('[root] No argument value provided. Terminating script.');
  process.exit(1); 
}

const insertHealthcareProvider = async (client, provider, state) => {
    const query = `
    INSERT INTO public.healthcare_providers (
      npi, npi_type, full_name, title, country, first_name, last_name, gender, nppes_created_at, nppes_updated_at, internal_updated_at, internal_created_at, sole_proprietor, years_experience, is_active, state, specialty
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
    ON CONFLICT (npi) DO UPDATE SET
      full_name = EXCLUDED.full_name,
      title = EXCLUDED.title,
      country = EXCLUDED.country,
      first_name = EXCLUDED.first_name,
      last_name = EXCLUDED.last_name,
      gender = EXCLUDED.gender,
      state = EXCLUDED.state,
      internal_updated_at = EXCLUDED.internal_updated_at,
      sole_proprietor = EXCLUDED.sole_proprietor,
      years_experience = EXCLUDED.years_experience,
      is_active = EXCLUDED.is_active
    RETURNING npi;
  `;

    // Calculate years of experience
    const enumerationDate = new Date(provider.basic.enumeration_date);
    const currentDate = new Date();
    const yearsExperience = Math.floor(
        (currentDate - enumerationDate) / (365.25 * 24 * 60 * 60 * 1000)
    );

    const values = [
        provider.number,
        provider.enumeration_type,
        `${provider.basic.name_prefix && provider.basic.name_prefix !== "--"
            ? provider.basic.name_prefix + " "
            : "" || provider.taxonomies.length > 0 ? "Dr. " : ""
        }${provider.basic.first_name} ${provider.basic.middle_name ? provider.basic.middle_name + " " : ""}${provider.basic.last_name}`,
        provider.basic.credential || null,
        provider.addresses[0].country_name,
        provider.basic.first_name,
        provider.basic.last_name,
        provider.basic.gender,
        provider.created_epoch,
        provider.last_updated_epoch,
        Math.floor(Date.now()),
        Math.floor(Date.now()),
        provider.basic.sole_proprietor,
        yearsExperience,
        provider.basic.status === "A" ? true : false,
        state,
        specialty
    ];

    const result = await client.query(query, values);
    return result.rows[0].npi;
};

const insertSpecialties = async (client, npi, taxonomies) => {
    const query = `
    INSERT INTO specialties (provider_npi, specialty, state, license, is_primary, code)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (provider_npi, specialty, license, state, code) DO NOTHING;
  `;

    for (const taxonomy of taxonomies) {
        const values = [
            npi,
            taxonomy.desc || "N/A",
            taxonomy.state,
            taxonomy.license,
            taxonomy.primary,
            taxonomy.code,
        ];
        await client.query(query, values);
    }
};

const insertInsurances = async (client, npi, insurances) => {
    const query = `
    INSERT INTO insurance_plans (provider_npi, insurance, identifier)
    VALUES ($1, $2, $3)
    ON CONFLICT (provider_npi, insurance, identifier) DO NOTHING;
  `;

    for (const insurance of insurances) {
        const values = [
            npi,
            insurance.issuer || insurance.desc,
            insurance.identifier,
        ];
        await client.query(query, values);
    }
};

const insertLocations = async (client, npi, addresses) => {
    const query = `
    INSERT INTO locations (provider_npi, address, city, state, phone, address_purpose, postal_code)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (provider_npi, address, city, state, address_purpose) DO UPDATE SET
    phone = EXCLUDED.phone;
  `;

    for (const address of addresses) {
        const values = [
            npi,
            `${address.address_1}${address.address_2 ? ", " + address.address_2 : ""
            }`,
            address.city,
            address.state,
            address.telephone_number,
            address.address_purpose,
            address.postal_code
        ];
        await client.query(query, values);
    }
};

function normalizeDataset(data) {
    return data.results.map((record) => {
        if (record.basic && record.basic.credential) {
            record.basic.credential = normalizeCredential(record.basic.credential);
        }
        return record;
    });
}

function normalizeCredential(credential) {
    if (!credential) return null;
    credential = credential.toUpperCase().replace(/\./g, "");
    let creds = credential.split(/[,\s]+/);
    creds = [...new Set(creds)];
    const dentalDegreePattern = /^[A-Z]{3,4}$/;
    const dentalDegree = creds.find((cred) => dentalDegreePattern.test(cred));
    if (dentalDegree) {
        creds = [dentalDegree, ...creds.filter((c) => c !== dentalDegree)];
    }

    return creds.join(", ");
}

// Main function to process each provider
const processProvider = async (client, provider, state) => {
    const npi = await insertHealthcareProvider(client, provider, state);
    await insertSpecialties(client, npi, provider.taxonomies);
    await insertInsurances(client, npi, provider.identifiers);

    const mappedPracticeLocations = provider.practiceLocations.map(location => ({
        ...location,
        address_purpose: `PRACTICE_${location.address_purpose}`
    }));
    await insertLocations(client, npi, [...provider.addresses, ...mappedPracticeLocations]);
};

// Function to fetch data from NPPES API
const fetchDoctorData = async (stateCode, skip, limit) => {
    const url = `${process.env.NPPES_API_URL}/?taxonomy_description=${specialty}&enumeration_type=NPI-1&state=${stateCode}&limit=${limit}&skip=${skip}&pretty=true&version=2.1`;
    logger.logBasic(`[fetchDoctorData] Fetching data for state: ${stateCode}, skip: ${skip}, limit: ${limit}`);
    try {
        const response = await axios.get(url);
        return response.data.results;
    } catch (error) {
        logger.logError('[fetchDoctorData] Error fetching data:', error.message);
        return [];
    }
};

// Main execution function
const executeIngestion = async () => {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        for (const state of usStates) {
            logger.logBasic('[executeIngestion] Processing state:', state.name);
            let allProviders = [];
            let skip = 0;
            const limit = 200;
            let hasMoreResults = true;

            const seenProviders = new Map();
            while (hasMoreResults) {
                const providers = await fetchDoctorData(state.code, skip, limit);
                if (providers.length === 0) {
                    hasMoreResults = false;
                } else {
                    const newProviders = providers.filter(provider => !seenProviders.has(provider.number));
                    if (newProviders.length === 0) {
                        hasMoreResults = false;
                    } else {
                        newProviders.forEach(provider => {
                            seenProviders.set(provider.number, true);
                            allProviders.push(provider);
                        });
                        skip += limit;
                    }
                }

                // Add a delay of 1 second between requests
                await new Promise(resolve => setTimeout(resolve, 1000));
            }

            const providers = allProviders;
            // Normalize the dataset
            const normalizedProviders = normalizeDataset({ results: providers });

            for (const provider of normalizedProviders) {
                await processProvider(client, provider, state.code);
            }

            logger.logBasic(`[executeIngestion] State ${state.name} processed. Waiting 2 seconds before next state...`);
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
        await client.query("COMMIT");
    } catch (error) {
        await client.query("ROLLBACK");
        logger.logError('[executeIngestion] Error during ingestion:', error);
    } finally {
        client.release();
    }
};

// Run the ingestion process
executeIngestion()
    .then(() => {
        logger.logPretty('[executeIngestion] Ingestion completed');
        pool.end();
    })
    .catch((error) => {
        logger.logError('[executeIngestion] Ingestion failed:', error);
        pool.end();
    });
