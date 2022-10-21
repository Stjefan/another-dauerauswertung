from dataclasses import dataclass, field

"""
ALTER TABLE "tsdb_lrpegel" ADD CONSTRAINT "tsdb_lrpegel_berechnet_von_id_fb69287e_fk_tsdb_ausw" FOREIGN KEY ("berechnet_von_id") REFERENCES "tsdb_auswertungslauf" ("id") DEFERRABLE INITIALLY DEFERRED;    """
"""
"""
"""
ALTER TABLE tsdb_lrpegel
drop CONSTRAINT tsdb_lrpegel_berechnet_von_id_fb69287e_fk_tsdb_ausw;

ALTER TABLE tsdb_lrpegel
ADD CONSTRAINT tsdb_lrpegel_berechnet_von_id_fb69287e_fk_tsdb_ausw
    FOREIGN KEY ("berechnet_von_id")
    REFERENCES tsdb_auswertungslauf
        ("id")
    ON DELETE CASCADE ON UPDATE NO ACTION;

    """


for i in [
    ("tsdb_lrpegel", "tsdb_lrpegel_berechnet_von_id_fb69287e_fk_tsdb_ausw"), (
    "tsdb_schallleistungpegel", "tsdb_schallleistungpegel_berechnet_von_id_a72a8946"),
    ("tsdb_rejected", "tsdb_rejected_berechnet_von_id_97029786_fk_tsdb_ausw"),
    ("tsdb_detected", "tsdb_detected_berechnet_von_id_3c016c9a"),
    ("tsdb_maxpegel", "tsdb_maxpegel_berechnet_von_id_b72f6d14_fk_tsdb_ausw")
    ]:
    tbl_name = i[0]
    constraint_name = i[1]
    my_str = f"""
    ALTER TABLE {tbl_name}
    drop CONSTRAINT {constraint_name};

    ALTER TABLE {tbl_name}
    ADD CONSTRAINT {constraint_name}
        FOREIGN KEY ("berechnet_von_id")
        REFERENCES tsdb_auswertungslauf
            ("id")
        ON DELETE CASCADE ON UPDATE NO ACTION;

    """
    print(my_str)