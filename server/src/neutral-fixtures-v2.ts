// Frozen historical v2 fixtures. Keep the old neutral-label migration reproducible.
export const V2_PROVIDERS = [
  [
    "provider-horizon",
    "Provider A",
    "Demo country A"
  ],
  [
    "provider-imizi",
    "Provider B",
    "Demo country B"
  ],
  [
    "provider-summit",
    "Provider C",
    "Demo country C"
  ]
] as const;
export const V2_SCHOOLS = [
  [
    "school-ke-01",
    "School A01",
    "Demo country A",
    "Region A1",
    "provider-horizon"
  ],
  [
    "school-ke-02",
    "School A02",
    "Demo country A",
    "Region A2",
    "provider-horizon"
  ],
  [
    "school-ke-03",
    "School A03",
    "Demo country A",
    "Region A3",
    "provider-horizon"
  ],
  [
    "school-ke-04",
    "School A04",
    "Demo country A",
    "Region A4",
    "provider-horizon"
  ],
  [
    "school-rw-01",
    "School B01",
    "Demo country B",
    "Region B1",
    "provider-imizi"
  ],
  [
    "school-rw-02",
    "School B02",
    "Demo country B",
    "Region B2",
    "provider-imizi"
  ],
  [
    "school-rw-03",
    "School B03",
    "Demo country B",
    "Region B3",
    "provider-imizi"
  ],
  [
    "school-rw-04",
    "School B04",
    "Demo country B",
    "Region B4",
    "provider-imizi"
  ],
  [
    "school-np-01",
    "School C01",
    "Demo country C",
    "Region C1",
    "provider-summit"
  ],
  [
    "school-np-02",
    "School C02",
    "Demo country C",
    "Region C2",
    "provider-summit"
  ],
  [
    "school-np-03",
    "School C03",
    "Demo country C",
    "Region C3",
    "provider-summit"
  ],
  [
    "school-np-04",
    "School C04",
    "Demo country C",
    "Region C4",
    "provider-summit"
  ]
] as const;
