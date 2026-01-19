/**
 * Airport Index - Client-side airport search
 * Data synced with demo_flights.db database (148 airports)
 */

window.AirportIndex = (function() {
    // Airport data matching the demo_flights.db database
    const AIRPORTS = [
        // Denmark
        { iata: 'AAL', city: 'Aalborg', country: 'Denmark', name: 'Aalborg Airport', lat: 57.094838, lng: 9.852853 },
        { iata: 'AAR', city: 'Aarhus', country: 'Denmark', name: 'Aarhus Airport', lat: 56.307566, lng: 10.624079 },
        { iata: 'BLL', city: 'Billund', country: 'Denmark', name: 'Billund Airport', lat: 55.740553, lng: 9.15219 },
        { iata: 'CPH', city: 'Copenhagen', country: 'Denmark', name: 'Copenhagen Airport', lat: 55.617962, lng: 12.653382 },

        // Turkey
        { iata: 'ADB', city: 'Izmir', country: 'Turkey', name: 'Adnan Menderes Airport', lat: 38.293081, lng: 27.154514 },
        { iata: 'AYT', city: 'Antalya', country: 'Turkey', name: 'Antalya Airport', lat: 36.901516, lng: 30.801188 },
        { iata: 'BJV', city: 'Bodrum', country: 'Turkey', name: 'Milas-Bodrum Airport', lat: 37.249627, lng: 27.664146 },
        { iata: 'DLM', city: 'Dalaman', country: 'Turkey', name: 'Dalaman Airport', lat: 36.715615, lng: 28.792908 },
        { iata: 'ESB', city: 'Ankara', country: 'Turkey', name: 'Esenboga Airport', lat: 40.12627, lng: 32.993385 },
        { iata: 'GZP', city: 'Gazipasa', country: 'Turkey', name: 'Gazipasa Airport', lat: 36.298346, lng: 32.301142 },
        { iata: 'IST', city: 'Istanbul', country: 'Turkey', name: 'Istanbul Airport', lat: 41.275278, lng: 28.751944 },
        { iata: 'SAW', city: 'Istanbul', country: 'Turkey', name: 'Sabiha Gokcen Airport', lat: 40.901986, lng: 29.31303 },
        { iata: 'TZX', city: 'Trabzon', country: 'Turkey', name: 'Trabzon Airport', lat: 40.994752, lng: 39.785667 },

        // Norway
        { iata: 'AES', city: 'Alesund', country: 'Norway', name: 'Alesund Airport', lat: 62.560566, lng: 6.117434 },
        { iata: 'BGO', city: 'Bergen', country: 'Norway', name: 'Bergen Airport', lat: 60.292615, lng: 5.220079 },
        { iata: 'BOO', city: 'Bodo', country: 'Norway', name: 'Bodo Airport', lat: 67.268756, lng: 14.363768 },
        { iata: 'EVE', city: 'Harstad', country: 'Norway', name: 'Harstad/Narvik Airport', lat: 68.490577, lng: 16.679554 },
        { iata: 'HAU', city: 'Haugesund', country: 'Norway', name: 'Haugesund Airport', lat: 59.344807, lng: 5.212219 },
        { iata: 'KRS', city: 'Kristiansand', country: 'Norway', name: 'Kristiansand Airport', lat: 58.204016, lng: 8.08457 },
        { iata: 'OSL', city: 'Oslo', country: 'Norway', name: 'Oslo Gardermoen Airport', lat: 60.196333, lng: 11.10041 },
        { iata: 'SVG', city: 'Stavanger', country: 'Norway', name: 'Stavanger Airport', lat: 58.878571, lng: 5.634591 },
        { iata: 'TOS', city: 'Tromso', country: 'Norway', name: 'Tromso Airport', lat: 69.682616, lng: 18.917582 },
        { iata: 'TRD', city: 'Trondheim', country: 'Norway', name: 'Trondheim Airport', lat: 63.458036, lng: 10.923299 },
        { iata: 'TRF', city: 'Sandefjord', country: 'Norway', name: 'Torp Airport', lat: 59.184571, lng: 10.257756 },

        // Iceland
        { iata: 'AEY', city: 'Akureyri', country: 'Iceland', name: 'Akureyri Airport', lat: 65.658308, lng: -18.073304 },
        { iata: 'EGS', city: 'Egilsstadir', country: 'Iceland', name: 'Egilsstadir Airport', lat: 65.27987, lng: -14.40552 },
        { iata: 'KEF', city: 'Reykjavik', country: 'Iceland', name: 'Keflavik Airport', lat: 63.977844, lng: -22.634763 },
        { iata: 'RKV', city: 'Reykjavik', country: 'Iceland', name: 'Reykjavik Airport', lat: 64.130054, lng: -21.93791 },

        // Netherlands
        { iata: 'AMS', city: 'Amsterdam', country: 'Netherlands', name: 'Schiphol Airport', lat: 52.308856, lng: 4.765869 },
        { iata: 'EIN', city: 'Eindhoven', country: 'Netherlands', name: 'Eindhoven Airport', lat: 51.450719, lng: 5.380664 },
        { iata: 'RTM', city: 'Rotterdam', country: 'Netherlands', name: 'Rotterdam The Hague Airport', lat: 51.956205, lng: 4.438551 },

        // Greece
        { iata: 'ATH', city: 'Athens', country: 'Greece', name: 'Athens International Airport', lat: 37.936023, lng: 23.946458 },
        { iata: 'CFU', city: 'Corfu', country: 'Greece', name: 'Corfu Airport', lat: 39.603532, lng: 19.912108 },
        { iata: 'CHQ', city: 'Chania', country: 'Greece', name: 'Chania Airport', lat: 35.538986, lng: 24.139999 },
        { iata: 'HER', city: 'Heraklion', country: 'Greece', name: 'Heraklion Airport', lat: 35.339659, lng: 25.178449 },
        { iata: 'JMK', city: 'Mykonos', country: 'Greece', name: 'Mykonos Airport', lat: 37.435345, lng: 25.347368 },
        { iata: 'JTR', city: 'Santorini', country: 'Greece', name: 'Santorini Airport', lat: 36.39976, lng: 25.478474 },
        { iata: 'KGS', city: 'Kos', country: 'Greece', name: 'Kos Airport', lat: 36.797396, lng: 27.090994 },
        { iata: 'RHO', city: 'Rhodes', country: 'Greece', name: 'Rhodes Airport', lat: 36.404674, lng: 28.087403 },
        { iata: 'SKG', city: 'Thessaloniki', country: 'Greece', name: 'Thessaloniki Airport', lat: 40.520061, lng: 22.971288 },
        { iata: 'ZTH', city: 'Zakynthos', country: 'Greece', name: 'Zakynthos Airport', lat: 37.752671, lng: 20.885506 },

        // Serbia
        { iata: 'BEG', city: 'Belgrade', country: 'Serbia', name: 'Belgrade Nikola Tesla Airport', lat: 44.820559, lng: 20.292378 },
        { iata: 'INI', city: 'Nis', country: 'Serbia', name: 'Nis Constantine the Great Airport', lat: 43.337429, lng: 21.866028 },

        // Germany
        { iata: 'BER', city: 'Berlin', country: 'Germany', name: 'Berlin Brandenburg Airport', lat: 52.362307, lng: 13.504906 },
        { iata: 'BRE', city: 'Bremen', country: 'Germany', name: 'Bremen Airport', lat: 53.047736, lng: 8.786273 },
        { iata: 'CGN', city: 'Cologne', country: 'Germany', name: 'Cologne Bonn Airport', lat: 50.868307, lng: 7.141736 },
        { iata: 'DUS', city: 'Dusseldorf', country: 'Germany', name: 'Dusseldorf Airport', lat: 51.28735, lng: 6.765655 },
        { iata: 'FRA', city: 'Frankfurt', country: 'Germany', name: 'Frankfurt Airport', lat: 50.034866, lng: 8.567755 },
        { iata: 'HAJ', city: 'Hanover', country: 'Germany', name: 'Hanover Airport', lat: 52.461492, lng: 9.687045 },
        { iata: 'HAM', city: 'Hamburg', country: 'Germany', name: 'Hamburg Airport', lat: 53.632011, lng: 9.992821 },
        { iata: 'HHN', city: 'Frankfurt', country: 'Germany', name: 'Frankfurt Hahn Airport', lat: 49.946527, lng: 7.263732 },
        { iata: 'MUC', city: 'Munich', country: 'Germany', name: 'Munich Airport', lat: 48.353732, lng: 11.780563 },
        { iata: 'NUE', city: 'Nuremberg', country: 'Germany', name: 'Nuremberg Airport', lat: 49.497171, lng: 11.078245 },
        { iata: 'STR', city: 'Stuttgart', country: 'Germany', name: 'Stuttgart Airport', lat: 48.688717, lng: 9.204025 },

        // United Kingdom
        { iata: 'BFS', city: 'Belfast', country: 'United Kingdom', name: 'Belfast International Airport', lat: 54.658279, lng: -6.217177 },
        { iata: 'BHD', city: 'Belfast', country: 'United Kingdom', name: 'Belfast City Airport', lat: 54.617857, lng: -5.872165 },
        { iata: 'BHX', city: 'Birmingham', country: 'United Kingdom', name: 'Birmingham Airport', lat: 52.453141, lng: -1.745768 },
        { iata: 'BQH', city: 'London', country: 'United Kingdom', name: 'Biggin Hill Airport', lat: 51.331399, lng: 0.030782 },
        { iata: 'BRS', city: 'Bristol', country: 'United Kingdom', name: 'Bristol Airport', lat: 51.383166, lng: -2.716318 },
        { iata: 'EDI', city: 'Edinburgh', country: 'United Kingdom', name: 'Edinburgh Airport', lat: 55.950392, lng: -3.366976 },
        { iata: 'GLA', city: 'Glasgow', country: 'United Kingdom', name: 'Glasgow Airport', lat: 55.870486, lng: -4.434056 },
        { iata: 'LCY', city: 'London', country: 'United Kingdom', name: 'London City Airport', lat: 51.505071, lng: 0.052398 },
        { iata: 'LGW', city: 'London', country: 'United Kingdom', name: 'Gatwick Airport', lat: 51.150882, lng: -0.18617 },
        { iata: 'LHR', city: 'London', country: 'United Kingdom', name: 'Heathrow Airport', lat: 51.470311, lng: -0.458118 },
        { iata: 'LTN', city: 'London', country: 'United Kingdom', name: 'Luton Airport', lat: 51.875482, lng: -0.37004 },
        { iata: 'MAN', city: 'Manchester', country: 'United Kingdom', name: 'Manchester Airport', lat: 53.35625, lng: -2.27384 },
        { iata: 'SEN', city: 'London', country: 'United Kingdom', name: 'Southend Airport', lat: 51.571075, lng: 0.697117 },
        { iata: 'STN', city: 'London', country: 'United Kingdom', name: 'Stansted Airport', lat: 51.885508, lng: 0.236933 },

        // Italy
        { iata: 'BGY', city: 'Milan', country: 'Italy', name: 'Milan Bergamo Airport', lat: 45.671735, lng: 9.703901 },
        { iata: 'BLQ', city: 'Bologna', country: 'Italy', name: 'Bologna Airport', lat: 44.53496, lng: 11.288279 },
        { iata: 'CIA', city: 'Rome', country: 'Italy', name: 'Rome Ciampino Airport', lat: 41.799344, lng: 12.593361 },
        { iata: 'CTA', city: 'Catania', country: 'Italy', name: 'Catania Airport', lat: 37.467052, lng: 15.066087 },
        { iata: 'FCO', city: 'Rome', country: 'Italy', name: 'Rome Fiumicino Airport', lat: 41.800082, lng: 12.242563 },
        { iata: 'MXP', city: 'Milan', country: 'Italy', name: 'Milan Malpensa Airport', lat: 45.630331, lng: 8.72682 },
        { iata: 'NAP', city: 'Naples', country: 'Italy', name: 'Naples Airport', lat: 40.884936, lng: 14.289355 },
        { iata: 'VCE', city: 'Venice', country: 'Italy', name: 'Venice Marco Polo Airport', lat: 45.50502, lng: 12.347639 },

        // Bosnia and Herzegovina
        { iata: 'BNX', city: 'Banja Luka', country: 'Bosnia and Herzegovina', name: 'Banja Luka Airport', lat: 44.939613, lng: 17.299061 },
        { iata: 'OMO', city: 'Mostar', country: 'Bosnia and Herzegovina', name: 'Mostar Airport', lat: 43.284727, lng: 17.842915 },
        { iata: 'SJJ', city: 'Sarajevo', country: 'Bosnia and Herzegovina', name: 'Sarajevo Airport', lat: 43.825342, lng: 18.334105 },
        { iata: 'TZL', city: 'Tuzla', country: 'Bosnia and Herzegovina', name: 'Tuzla Airport', lat: 44.458664, lng: 18.724986 },

        // France
        { iata: 'BOD', city: 'Bordeaux', country: 'France', name: 'Bordeaux Airport', lat: 44.829447, lng: -0.71293 },
        { iata: 'BVA', city: 'Paris', country: 'France', name: 'Paris Beauvais Airport', lat: 49.455688, lng: 2.111972 },
        { iata: 'CDG', city: 'Paris', country: 'France', name: 'Charles de Gaulle Airport', lat: 49.011244, lng: 2.548962 },
        { iata: 'LBG', city: 'Paris', country: 'France', name: 'Paris Le Bourget Airport', lat: 48.965436, lng: 2.439296 },
        { iata: 'LIL', city: 'Lille', country: 'France', name: 'Lille Airport', lat: 50.568372, lng: 3.097166 },
        { iata: 'LYS', city: 'Lyon', country: 'France', name: 'Lyon Airport', lat: 45.723723, lng: 5.082626 },
        { iata: 'MRS', city: 'Marseille', country: 'France', name: 'Marseille Airport', lat: 43.438847, lng: 5.217919 },
        { iata: 'NCE', city: 'Nice', country: 'France', name: 'Nice Cote d\'Azur Airport', lat: 43.659265, lng: 7.212948 },
        { iata: 'NTE', city: 'Nantes', country: 'France', name: 'Nantes Airport', lat: 47.155308, lng: -1.60848 },
        { iata: 'ORY', city: 'Paris', country: 'France', name: 'Paris Orly Airport', lat: 48.726427, lng: 2.362898 },
        { iata: 'POX', city: 'Paris', country: 'France', name: 'Paris Pontoise Airport', lat: 49.09937, lng: 2.039917 },
        { iata: 'SXB', city: 'Strasbourg', country: 'France', name: 'Strasbourg Airport', lat: 48.540226, lng: 7.628039 },
        { iata: 'TLS', city: 'Toulouse', country: 'France', name: 'Toulouse Airport', lat: 43.629243, lng: 1.365751 },
        { iata: 'XCR', city: 'Chalons', country: 'France', name: 'Paris Vatry Airport', lat: 48.780471, lng: 4.189426 },

        // Belgium
        { iata: 'BRU', city: 'Brussels', country: 'Belgium', name: 'Brussels Airport', lat: 50.90042, lng: 4.484052 },
        { iata: 'CRL', city: 'Brussels', country: 'Belgium', name: 'Brussels South Charleroi Airport', lat: 50.46129, lng: 4.457859 },

        // Switzerland
        { iata: 'BSL', city: 'Basel', country: 'Switzerland', name: 'Basel Airport', lat: 47.59409, lng: 7.52733 },
        { iata: 'GVA', city: 'Geneva', country: 'Switzerland', name: 'Geneva Airport', lat: 46.237553, lng: 6.109053 },
        { iata: 'ZRH', city: 'Zurich', country: 'Switzerland', name: 'Zurich Airport', lat: 47.461458, lng: 8.552323 },

        // Hungary
        { iata: 'BUD', city: 'Budapest', country: 'Hungary', name: 'Budapest Ferenc Liszt Airport', lat: 47.437552, lng: 19.25574 },

        // Romania
        { iata: 'CLJ', city: 'Cluj-Napoca', country: 'Romania', name: 'Cluj-Napoca Airport', lat: 46.783742, lng: 23.686859 },
        { iata: 'IAS', city: 'Iasi', country: 'Romania', name: 'Iasi Airport', lat: 47.177434, lng: 27.62002 },
        { iata: 'OTP', city: 'Bucharest', country: 'Romania', name: 'Bucharest Henri Coanda Airport', lat: 44.57092, lng: 26.084706 },
        { iata: 'TSR', city: 'Timisoara', country: 'Romania', name: 'Timisoara Airport', lat: 45.809901, lng: 21.332228 },

        // Ireland
        { iata: 'DUB', city: 'Dublin', country: 'Ireland', name: 'Dublin Airport', lat: 53.425503, lng: -6.249001 },
        { iata: 'NOC', city: 'Knock', country: 'Ireland', name: 'Ireland West Airport Knock', lat: 53.912018, lng: -8.814867 },
        { iata: 'ORK', city: 'Cork', country: 'Ireland', name: 'Cork Airport', lat: 51.844147, lng: -8.491224 },
        { iata: 'SNN', city: 'Shannon', country: 'Ireland', name: 'Shannon Airport', lat: 52.700828, lng: -8.919755 },

        // Poland
        { iata: 'GDN', city: 'Gdansk', country: 'Poland', name: 'Gdansk Lech Walesa Airport', lat: 54.378221, lng: 18.467137 },
        { iata: 'KRK', city: 'Krakow', country: 'Poland', name: 'Krakow John Paul II Airport', lat: 50.07734, lng: 19.786459 },
        { iata: 'KTW', city: 'Katowice', country: 'Poland', name: 'Katowice Airport', lat: 50.47355, lng: 19.07794 },
        { iata: 'LUZ', city: 'Lublin', country: 'Poland', name: 'Lublin Airport', lat: 51.238788, lng: 22.714096 },
        { iata: 'POZ', city: 'Poznan', country: 'Poland', name: 'Poznan Airport', lat: 52.420476, lng: 16.827458 },
        { iata: 'RZE', city: 'Rzeszow', country: 'Poland', name: 'Rzeszow Airport', lat: 50.111666, lng: 22.02361 },
        { iata: 'SZZ', city: 'Szczecin', country: 'Poland', name: 'Szczecin Airport', lat: 53.585289, lng: 14.90249 },
        { iata: 'WAW', city: 'Warsaw', country: 'Poland', name: 'Warsaw Chopin Airport', lat: 52.166211, lng: 20.967363 },
        { iata: 'WRO', city: 'Wroclaw', country: 'Poland', name: 'Wroclaw Airport', lat: 51.103432, lng: 16.883366 },

        // Austria
        { iata: 'GRZ', city: 'Graz', country: 'Austria', name: 'Graz Airport', lat: 46.992573, lng: 15.439865 },
        { iata: 'INN', city: 'Innsbruck', country: 'Austria', name: 'Innsbruck Airport', lat: 47.260188, lng: 11.345793 },
        { iata: 'KLU', city: 'Klagenfurt', country: 'Austria', name: 'Klagenfurt Airport', lat: 46.649659, lng: 4.325341 },
        { iata: 'LNZ', city: 'Linz', country: 'Austria', name: 'Linz Airport', lat: 48.235324, lng: 14.188682 },
        { iata: 'SZG', city: 'Salzburg', country: 'Austria', name: 'Salzburg Airport', lat: 47.792973, lng: 13.003613 },
        { iata: 'VIE', city: 'Vienna', country: 'Austria', name: 'Vienna International Airport', lat: 48.113065, lng: 16.568137 },

        // Czech Republic
        { iata: 'PRG', city: 'Prague', country: 'Czech Republic', name: 'Prague Vaclav Havel Airport', lat: 50.101295, lng: 14.26159 },

        // Cyprus
        { iata: 'LCA', city: 'Larnaca', country: 'Cyprus', name: 'Larnaca Airport', lat: 34.873719, lng: 33.622626 },
        { iata: 'PFO', city: 'Paphos', country: 'Cyprus', name: 'Paphos Airport', lat: 34.715181, lng: 32.48722 },

        // Lithuania
        { iata: 'KUN', city: 'Kaunas', country: 'Lithuania', name: 'Kaunas Airport', lat: 54.964848, lng: 24.079525 },
        { iata: 'PLQ', city: 'Palanga', country: 'Lithuania', name: 'Palanga Airport', lat: 55.97271, lng: 21.094658 },
        { iata: 'VNO', city: 'Vilnius', country: 'Lithuania', name: 'Vilnius Airport', lat: 54.636069, lng: 25.286179 },

        // Latvia
        { iata: 'RIX', city: 'Riga', country: 'Latvia', name: 'Riga Airport', lat: 56.923127, lng: 23.972206 },

        // Estonia
        { iata: 'TAY', city: 'Tartu', country: 'Estonia', name: 'Tartu Airport', lat: 58.308502, lng: 26.689866 },
        { iata: 'TLL', city: 'Tallinn', country: 'Estonia', name: 'Tallinn Airport', lat: 59.413576, lng: 24.832857 },

        // Finland
        { iata: 'HEL', city: 'Helsinki', country: 'Finland', name: 'Helsinki-Vantaa Airport', lat: 60.31912, lng: 24.95808 },
        { iata: 'OUL', city: 'Oulu', country: 'Finland', name: 'Oulu Airport', lat: 64.930487, lng: 25.355219 },
        { iata: 'RVN', city: 'Rovaniemi', country: 'Finland', name: 'Rovaniemi Airport', lat: 66.56395, lng: 25.830239 },
        { iata: 'TKU', city: 'Turku', country: 'Finland', name: 'Turku Airport', lat: 60.512798, lng: 22.263042 },
        { iata: 'TMP', city: 'Tampere', country: 'Finland', name: 'Tampere-Pirkkala Airport', lat: 61.414478, lng: 23.604515 },

        // Sweden
        { iata: 'VBY', city: 'Visby', country: 'Sweden', name: 'Visby Airport', lat: 57.661882, lng: 18.344675 },

        // Luxembourg
        { iata: 'LUX', city: 'Luxembourg', country: 'Luxembourg', name: 'Luxembourg Airport', lat: 49.626117, lng: 6.209594 },

        // Malta
        { iata: 'MLA', city: 'Malta', country: 'Malta', name: 'Malta International Airport', lat: 35.855806, lng: 14.48039 },

        // Montenegro
        { iata: 'TGD', city: 'Podgorica', country: 'Montenegro', name: 'Podgorica Airport', lat: 42.359323, lng: 19.251822 },
        { iata: 'TIV', city: 'Tivat', country: 'Montenegro', name: 'Tivat Airport', lat: 42.40487, lng: 18.722972 },

        // Albania
        { iata: 'TIA', city: 'Tirana', country: 'Albania', name: 'Tirana International Airport', lat: 41.414729, lng: 19.719376 },

        // North Macedonia
        { iata: 'OHD', city: 'Ohrid', country: 'North Macedonia', name: 'Ohrid St. Paul the Apostle Airport', lat: 41.18058, lng: 20.743799 },
        { iata: 'SKP', city: 'Skopje', country: 'North Macedonia', name: 'Skopje International Airport', lat: 41.96181, lng: 21.623086 },

        // Kosovo
        { iata: 'PRN', city: 'Pristina', country: 'Kosovo', name: 'Pristina International Airport', lat: 42.573996, lng: 21.033062 },

        // Moldova
        { iata: 'RMO', city: 'Chisinau', country: 'Moldova', name: 'Chisinau International Airport', lat: 46.931503, lng: 28.933121 },

        // Paris area (additional)
        { iata: 'VIY', city: 'Paris', country: 'France', name: 'Paris Villacoublay Airport', lat: 48.774167, lng: 2.191667 },
    ];

    let _airports = [];
    let _searchIndex = [];
    let _loaded = false;
    let _loadPromise = null;

    function _buildSearchIndex() {
        _searchIndex = _airports.map(a => ({
            ...a,
            // Only search by city and country (not IATA or airport name)
            searchText: `${a.city} ${a.country}`.toLowerCase()
        }));
    }

    return {
        /**
         * Load airport data. In production, fetches from API.
         * For MVP, uses static data immediately.
         * @returns {Promise<void>}
         */
        async load() {
            if (_loaded) return;
            if (_loadPromise) return _loadPromise;

            _loadPromise = new Promise((resolve) => {
                _airports = AIRPORTS;
                _buildSearchIndex();
                _loaded = true;
                resolve();
            });

            return _loadPromise;
        },

        /**
         * Search airports by query.
         * @param {string} query - Search query
         * @param {number} limit - Max results
         * @returns {Array} - Matching airports with scores
         */
        search(query, limit = 8) {
            if (!_loaded) return [];

            const q = query.toLowerCase().trim();
            if (q.length < 1) return [];

            const results = _searchIndex
                .map(airport => {
                    let score = 0;
                    const cityLower = airport.city.toLowerCase();
                    const countryLower = airport.country.toLowerCase();

                    // City exact match (highest priority)
                    if (cityLower === q) {
                        score += 100;
                    }
                    // City starts with query
                    else if (cityLower.startsWith(q)) {
                        score += 50;
                    }
                    // City contains query
                    else if (cityLower.includes(q)) {
                        score += 30;
                    }
                    // Country starts with query
                    else if (countryLower.startsWith(q)) {
                        score += 20;
                    }
                    // Country contains query
                    else if (countryLower.includes(q)) {
                        score += 10;
                    }

                    return { ...airport, score };
                })
                .filter(a => a.score > 0)
                .sort((a, b) => b.score - a.score)
                .slice(0, limit);

            return results;
        },

        /**
         * Validate an IATA code exists.
         * @param {string} iata - IATA code to validate
         * @returns {boolean}
         */
        validate(iata) {
            if (!_loaded) return false;
            return _airports.some(a => a.iata.toUpperCase() === iata.toUpperCase());
        },

        /**
         * Get airport by IATA code.
         * @param {string} iata - IATA code
         * @returns {Object|null}
         */
        getByIata(iata) {
            if (!_loaded) return null;
            return _airports.find(a => a.iata.toUpperCase() === iata.toUpperCase()) || null;
        },

        /**
         * Get all airports.
         * @returns {Array}
         */
        getAll() {
            return [..._airports];
        },

        /**
         * Check if loaded.
         * @returns {boolean}
         */
        isLoaded() {
            return _loaded;
        }
    };
})();
