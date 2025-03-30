create schema statistic
    create table statistic.reward_type(
                                          id serial primary key ,
                                          type text -- []string{"Лучший игрок месяца по рейтингу!"}
    )

create table statistic.reward(
                                 id serial primary key ,
                                 user_id serial references account.user(id),
                                 year varchar(4) not null ,
                                 month varchar(25) not null ,
                                 type serial references statistic.reward_type(id),
                                 value text,
                                 created_at timestamp default now()
);
INSERT INTO statistic.reward_type (type) VALUES ('Лучший игрок месяца по рейтингу 1x1!');
INSERT INTO statistic.reward_type (type) VALUES ('Лучший игрок месяца по рейтингу 2x2!');
INSERT INTO statistic.reward_type (type) VALUES ('Лучший игрок месяца по рейтингу 3x3!');
INSERT INTO statistic.reward_type (type) VALUES ('Лучший игрок месяца по рейтингу 4x4!');
INSERT INTO statistic.reward_type (type) VALUES ('Лучший игрок месяца по рейтингу 5x5!');
INSERT INTO statistic.reward_type (type) VALUES ('Худший игрок месяца по рейтингу 1x1!');
INSERT INTO statistic.reward_type (type) VALUES ('Худший игрок месяца по рейтингу 2x2!');
INSERT INTO statistic.reward_type (type) VALUES ('Худший игрок месяца по рейтингу 3x3!');
INSERT INTO statistic.reward_type (type) VALUES ('Худший игрок месяца по рейтингу 4x4!');
INSERT INTO statistic.reward_type (type) VALUES ('Худший игрок месяца по рейтингу 5x5!');
INSERT INTO statistic.reward_type (type) VALUES ('Лучший процент побед за месяц!');
INSERT INTO statistic.reward_type (type) VALUES ('Худший процент побед за месяц!');
INSERT INTO statistic.reward_type (type) VALUES ('Максимальный прирост рейтинга за месяц!');
INSERT INTO statistic.reward_type (type) VALUES ('Максимальная потеря рейтинга за месяц!');
INSERT INTO statistic.reward_type (type) VALUES ('Наибольшее количество сыгранных игр за месяц!');
INSERT INTO statistic.reward_type (type) VALUES ('Самая длинная серия подряд за месяц!');